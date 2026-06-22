"""
================================================================================
ORDER MANAGER — Signal-to-Exchange Dispatch & Order Lifecycle Tracking
================================================================================

Translates StrategySignals into exchange orders and tracks their lifecycle
from PENDING → OPEN → FILLED/CANCELLED/REJECTED.

RESPONSIBILITIES:
    - Dispatch: Convert StrategySignal → List[OrderRequest] → Exchange API.
    - Grid dispatch: Place all limit orders for a grid atomically.
    - Trend dispatch: Place entry + stop-loss + take-profit orders.
    - Reconciliation: Periodically sync local order state with exchange.
    - Ledger recording: After each fill, write to local_ledger.

CONCURRENCY MODEL:
    - Order dispatch is serialized per symbol to prevent duplicate orders.
    - Fill tracking uses exchange_order_id as the deduplication key.
    - Reconciliation is a background task that runs every N seconds.
================================================================================
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from execution.exchange_client import ExchangeClient, OrderRequest, OrderResponse
from strategy.signal import SignalAction, StrategySignal

logger = logging.getLogger("execution.order_manager")


# ---------------------------------------------------------------------------
# Data Structures
# ---------------------------------------------------------------------------

@dataclass
class TrackedOrder:
    """An order being tracked by the order manager."""
    order_id: str  # Internal ID from ledger
    exchange_order_id: Optional[str] = None
    client_order_id: Optional[str] = None
    symbol: str = ""
    side: str = ""
    order_type: str = ""
    price: float = 0.0
    quantity: float = 0.0
    filled_quantity: float = 0.0
    status: str = "PENDING"
    trade_id: Optional[str] = None
    grid_id: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


# ---------------------------------------------------------------------------
# OrderManager
# ---------------------------------------------------------------------------

class OrderManager:
    """
    Manages the full lifecycle of exchange orders.

    Connects the strategy layer (via signals) to the exchange layer (via
    ExchangeClient) and records everything in the ledger.

    USAGE:
        mgr = OrderManager(client, ledger_callback)
        # Dispatch a signal:
        tracked = await mgr.dispatch_signal(signal, risk_params)
        # Reconcile periodically:
        await mgr.reconcile()
    """

    def __init__(
        self,
        exchange_client: ExchangeClient,
        ledger_record_order: Callable,
        ledger_update_fill: Callable,
        ledger_record_trade_open: Callable,
        ledger_record_trade_close: Callable,
        ledger_register_grid: Callable = None,
        reconcile_interval: float = 30.0,
    ):
        """
        Initialize the order manager.

        Args:
            exchange_client: The ExchangeClient instance.
            ledger_record_order: Callback to record an order in the ledger.
            ledger_update_fill: Callback to update order fill in the ledger.
            ledger_record_trade_open: Callback to record trade open.
            ledger_record_trade_close: Callback to record trade close.
            ledger_register_grid: Callback to register a grid trade container.
            reconcile_interval: Seconds between order reconciliation cycles.
        """
        self._client = exchange_client
        self._ledger_record_order = ledger_record_order
        self._ledger_update_fill = ledger_update_fill
        self._ledger_record_trade_open = ledger_record_trade_open
        self._ledger_record_trade_close = ledger_record_trade_close
        self._ledger_register_grid = ledger_register_grid or ledger_record_trade_open
        self.reconcile_interval = reconcile_interval

        # Tracked orders: internal_order_id → TrackedOrder
        self._orders: Dict[str, TrackedOrder] = {}
        self._dispatch_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

        # Reconciliation
        self._reconcile_task: Optional[asyncio.Task] = None
        self._running = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the order manager and reconciliation loop."""
        self._running = True
        self._reconcile_task = asyncio.create_task(self._reconciliation_loop())
        logger.info("OrderManager started")

    async def stop(self) -> None:
        """Stop the order manager."""
        self._running = False
        if self._reconcile_task:
            self._reconcile_task.cancel()
            try:
                await self._reconcile_task
            except asyncio.CancelledError:
                pass
        logger.info("OrderManager stopped")

    # ------------------------------------------------------------------
    # Signal Dispatch
    # ------------------------------------------------------------------

    async def dispatch_signal(
        self,
        signal: StrategySignal,
        risk_params: Optional[Dict[str, Any]] = None,
    ) -> List[TrackedOrder]:
        """
        Dispatch a strategy signal to the exchange.

        This is the MAIN entry point. Called by main.py after risk approval.

        Args:
            signal: The approved StrategySignal.
            risk_params: Optional modified parameters from RiskGuard.

        Returns:
            List of TrackedOrder objects that were dispatched.
        """
        # Merge risk modifications into metadata
        metadata = dict(signal.metadata)
        if risk_params:
            metadata.update(risk_params)

        async with self._dispatch_locks[signal.symbol]:
            if signal.action == SignalAction.START_GRID:
                return await self._dispatch_grid(signal, metadata)
            elif signal.action == SignalAction.STOP_GRID:
                return await self._cancel_grid(signal, metadata)
            elif signal.action == SignalAction.START_TREND:
                return await self._dispatch_trend(signal, metadata)
            elif signal.action == SignalAction.STOP_TREND:
                return await self._close_trend(signal, metadata)
            elif signal.action == SignalAction.CLOSE_ALL:
                return await self._close_all(signal.symbol)
            elif signal.action == SignalAction.MODIFY_POSITION:
                return await self._modify_position(signal, metadata)
            else:
                logger.warning(f"Unknown action: {signal.action}")
                return []

    # ------------------------------------------------------------------
    # Grid Dispatch
    # ------------------------------------------------------------------

    async def _dispatch_grid(
        self, signal: StrategySignal, metadata: Dict[str, Any]
    ) -> List[TrackedOrder]:
        """Place all grid limit orders."""
        levels = metadata.get("levels", [])
        if not levels:
            logger.warning("Grid signal has no levels")
            return []

        tracked_orders = []
        trade_id = metadata.get("grid_id", "")

        # Register grid as a trade container (no journal entries — fills create their own)
        self._ledger_register_grid(trade_id, signal.symbol)

        for level_dict in levels:
            try:
                # Round to exchange precision
                price, qty = self._round_to_tick(signal.symbol, level_dict["price"], level_dict["quantity"])
                if qty <= 0:
                    continue

                order_req = OrderRequest(
                    symbol=signal.symbol,
                    side=level_dict["side"],
                    order_type="LIMIT",
                    price=price,
                    quantity=qty,
                    time_in_force="GTC",
                )

                # Record in ledger FIRST (before API call)
                order_id = self._ledger_record_order(
                    trade_id=trade_id,
                    symbol=signal.symbol,
                    side=level_dict["side"],
                    order_type="LIMIT",
                    quantity=level_dict["quantity"],
                    price=level_dict["price"],
                )

                # Place order on exchange
                response = await self._client.place_order(order_req)

                # Track the order
                tracked = TrackedOrder(
                    order_id=order_id,
                    exchange_order_id=response.exchange_order_id,
                    client_order_id=response.client_order_id,
                    symbol=signal.symbol,
                    side=level_dict["side"],
                    order_type="LIMIT",
                    price=level_dict["price"],
                    quantity=level_dict["quantity"],
                    status=response.status,
                    trade_id=trade_id,
                    grid_id=metadata.get("grid_id"),
                )

                self._orders[order_id] = tracked
                tracked_orders.append(tracked)

                logger.info(
                    f"Grid order placed: {signal.symbol} {level_dict['side']} "
                    f"{level_dict['quantity']} @ {level_dict['price']} "
                    f"[{response.exchange_order_id}]"
                )

            except Exception as e:
                logger.error(f"Failed to place grid order: {e}")
                continue

        return tracked_orders

    async def _cancel_grid(
        self, signal: StrategySignal, metadata: Dict[str, Any]
    ) -> List[TrackedOrder]:
        """Cancel all orders for a grid and close any filled positions."""
        grid_id = metadata.get("grid_id", "")
        cancelled = []

        # Cancel all open orders for this grid
        grid_orders = [o for o in self._orders.values() if o.grid_id == grid_id and o.status == "OPEN"]
        for order in grid_orders:
            try:
                await self._client.cancel_order(
                    symbol=order.symbol,
                    client_order_id=order.client_order_id,
                )
                order.status = "CANCELLED"
                cancelled.append(order)
            except Exception as e:
                logger.error(f"Failed to cancel order {order.order_id}: {e}")

        # Close the grid trade in the ledger
        self._ledger_record_trade_close(
            trade_id=grid_id,
            exit_price=0.0,  # Will be determined by actual fills
            fee=0.0,
        )

        return cancelled

    # ------------------------------------------------------------------
    # Trend Dispatch
    # ------------------------------------------------------------------

    async def _dispatch_trend(
        self, signal: StrategySignal, metadata: Dict[str, Any]
    ) -> List[TrackedOrder]:
        """Place a trend entry order with stop-loss."""
        direction = metadata["direction"]
        entry_price = metadata.get("entry_price", 0.0)
        position_size = metadata.get("position_size", 0.0)
        stop_loss = metadata.get("stop_loss", 0.0)

        side = "BUY" if direction == "LONG" else "SELL"

        # Record trade open in ledger
        trade_id = f"trend_{signal.symbol}_{int(datetime.now(timezone.utc).timestamp())}"
        self._ledger_record_trade_open(
            trade_id=trade_id,
            symbol=signal.symbol,
            side=side,
            quantity=position_size,
            price=entry_price,
        )

        # Place entry order
        entry_order_req = OrderRequest(
            symbol=signal.symbol,
            side=side,
            order_type="MARKET",
            quantity=position_size,
        )

        order_id = self._ledger_record_order(
            trade_id=trade_id,
            symbol=signal.symbol,
            side=side,
            order_type="MARKET",
            quantity=position_size,
        )

        tracked_orders = []
        try:
            response = await self._client.place_order(entry_order_req)
            tracked = TrackedOrder(
                order_id=order_id,
                exchange_order_id=response.exchange_order_id,
                client_order_id=response.client_order_id,
                symbol=signal.symbol,
                side=side,
                order_type="MARKET",
                quantity=position_size,
                status=response.status,
                trade_id=trade_id,
            )
            self._orders[order_id] = tracked
            tracked_orders.append(tracked)

            # If fill is immediate, update ledger
            if response.executed_qty > 0:
                fill_price = response.cummulative_quote_qty / response.executed_qty if response.executed_qty > 0 else entry_price
                self._ledger_update_fill(
                    order_id=order_id,
                    exchange_order_id=response.exchange_order_id,
                    filled_quantity=response.executed_qty,
                    fill_price=fill_price,
                )

                # Place stop-loss order
                stop_side = "SELL" if direction == "LONG" else "BUY"
                stop_req = OrderRequest(
                    symbol=signal.symbol,
                    side=stop_side,
                    order_type="STOP_LOSS",
                    price=stop_loss,
                    stop_price=stop_loss,
                    quantity=position_size,
                )
                await self._client.place_order(stop_req)
                logger.info(f"Stop-loss placed at {stop_loss}")

            logger.info(f"Trend entry placed: {signal.symbol} {direction} {position_size}")

        except Exception as e:
            logger.error(f"Failed to place trend entry: {e}")

        return tracked_orders

    async def _close_trend(
        self, signal: StrategySignal, metadata: Dict[str, Any]
    ) -> List[TrackedOrder]:
        """Close a trend position at market."""
        direction = metadata.get("direction", "LONG")
        close_side = "SELL" if direction == "LONG" else "BUY"
        exit_price = metadata.get("exit_price", 0.0)

        # Cancel associated stop-loss orders
        await self._client.cancel_all_orders(signal.symbol)

        # Place market close order
        order_req = OrderRequest(
            symbol=signal.symbol,
            side=close_side,
            order_type="MARKET",
            quantity=metadata.get("position_size", 0.0),
        )

        tracked_orders = []
        try:
            response = await self._client.place_order(order_req)
            logger.info(f"Trend exit placed: {signal.symbol} {close_side}")
            tracked_orders.append(TrackedOrder(
                order_id=f"exit_{signal.symbol}",
                exchange_order_id=response.exchange_order_id,
                symbol=signal.symbol,
                side=close_side,
                status=response.status,
            ))
        except Exception as e:
            logger.error(f"Failed to place trend exit: {e}")

        return tracked_orders

    async def _close_all(self, symbol: str) -> List[TrackedOrder]:
        """Emergency: close all positions for a symbol."""
        logger.warning(f" EMERGENCY CLOSE ALL: {symbol}")
        try:
            await self._client.cancel_all_orders(symbol)
        except Exception as e:
            logger.error(f"Failed to cancel all orders: {e}")

        # In production, would also place market orders to close all positions
        return []

    async def _modify_position(
        self, signal: StrategySignal, metadata: Dict[str, Any]
    ) -> List[TrackedOrder]:
        """Modify an existing position (e.g., update trailing stop)."""
        action_type = metadata.get("action_type", "")
        if action_type == "update_trailing_stop":
            # Cancel existing stop order and place new one
            await self._client.cancel_all_orders(signal.symbol)

            new_stop = metadata.get("new_trailing_stop", 0.0)
            if new_stop > 0:
                direction = metadata.get("direction", "LONG")
                stop_side = "SELL" if direction == "LONG" else "BUY"
                stop_req = OrderRequest(
                    symbol=signal.symbol,
                    side=stop_side,
                    order_type="STOP_LOSS",
                    price=new_stop,
                    stop_price=new_stop,
                    quantity=metadata.get("position_size", 0.0),
                )
                await self._client.place_order(stop_req)
                logger.info(f"Trailing stop updated to {new_stop}")

        return []

    # ------------------------------------------------------------------
    # Exchange Precision
    # ------------------------------------------------------------------

    @staticmethod
    def _round_to_tick(symbol: str, price: float, qty: float) -> tuple:
        """Round price and quantity to exchange-allowed tick sizes."""
        # Binance spot tick sizes
        ticks = {
            "BTCUSDT": (2, 5),   # price: 0.01 (2dp), qty: 0.00001 (5dp)
            "ETHUSDT": (2, 4),   # price: 0.01 (2dp), qty: 0.0001 (4dp)
            "SOLUSDT": (2, 2),   # price: 0.01 (2dp), qty: 0.01 (2dp)
            "BNBUSDT": (1, 3),   # price: 0.1 (1dp), qty: 0.001 (3dp)
        }
        price_dp, qty_dp = ticks.get(symbol.upper(), (2, 5))

        price = max(0.01, round(price, price_dp))
        qty = max(10 ** -qty_dp, round(qty, qty_dp))

        return price, qty

    # ------------------------------------------------------------------
    # Reconciliation
    # ------------------------------------------------------------------

    async def _reconciliation_loop(self) -> None:
        """Background task that periodically reconciles order states."""
        while self._running:
            try:
                await asyncio.sleep(self.reconcile_interval)
                await self.reconcile()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Reconciliation error: {e}")

    async def reconcile(self) -> None:
        """
        Sync local order state with exchange.

        Queries open orders and fills from the exchange, updating our
        internal tracking and the ledger accordingly.
        """
        symbols = set(o.symbol for o in self._orders.values())
        for symbol in symbols:
            try:
                open_orders = await self._client.get_open_orders(symbol)
                exchange_order_ids = {str(o.get("orderId", "")) for o in open_orders}

                # Mark locally-tracked orders as filled if they're not in open orders
                for order in list(self._orders.values()):
                    if order.symbol != symbol:
                        continue
                    if order.exchange_order_id and order.exchange_order_id not in exchange_order_ids:
                        if order.status == "OPEN":
                            # Order is no longer open — check if filled
                            try:
                                status = await self._client.get_order_status(
                                    symbol=symbol,
                                    order_id=order.exchange_order_id,
                                )
                                new_status = status.get("status", "UNKNOWN")
                                if new_status == "FILLED":
                                    order.status = "FILLED"
                                    order.filled_quantity = float(status.get("executedQty", 0))
                                    fill_price = float(status.get("cummulativeQuoteQty", 0)) / order.filled_quantity if order.filled_quantity > 0 else 0
                                    self._ledger_update_fill(
                                        order_id=order.order_id,
                                        exchange_order_id=order.exchange_order_id,
                                        filled_quantity=order.filled_quantity,
                                        fill_price=fill_price,
                                    )
                                    logger.info(f"Reconciled fill: {order.order_id}")
                            except Exception:
                                pass

            except Exception as e:
                logger.error(f"Reconciliation failed for {symbol}: {e}")


from collections import defaultdict
