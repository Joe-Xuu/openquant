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
import os
import time
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
        ledger_order_open: Callable = None,
        ledger_update_fill: Callable = None,
        ledger_record_trade_open: Callable = None,
        ledger_record_trade_close: Callable = None,
        ledger_register_grid: Callable = None,
        reconcile_interval: float = 5.0,
    ):
        """
        Initialize the order manager.

        Args:
            exchange_client: The ExchangeClient instance.
            ledger_record_order: Callback to record an order in the ledger.
            ledger_order_open: Callback to update order status to OPEN.
            ledger_update_fill: Callback to update order fill in the ledger.
            ledger_record_trade_open: Callback to record trade open.
            ledger_record_trade_close: Callback to record trade close.
            ledger_register_grid: Callback to register a grid trade container.
            reconcile_interval: Seconds between order reconciliation cycles.
        """
        self._client = exchange_client
        self._ledger_record_order = ledger_record_order
        self._ledger_order_open = ledger_order_open or (lambda oid, eid: None)
        self._ledger_update_fill = ledger_update_fill or (lambda *a, **kw: None)
        self._ledger_record_trade_open = ledger_record_trade_open or (lambda *a, **kw: "")
        self._ledger_record_trade_close = ledger_record_trade_close or (lambda *a, **kw: (0,0))
        self._ledger_register_grid = ledger_register_grid or (lambda tid, sym: tid)
        self.reconcile_interval = reconcile_interval
        self.grid_active = False
        self.grid_deployed_at: float = 0.0  # timestamp when grid went live
        self._consecutive_buys: int = 0     # buys without a sell in between
        self._consecutive_buy_limit: int = 5  # pause grid after this many
        self._counted_fills: set = set()    # trade IDs already counted for circuit breaker

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
        """Place all grid limit orders — cancels old ones first (rebalance-safe)."""
        levels = metadata.get("levels", [])
        if not levels:
            logger.warning("Grid signal has no levels")
            return []

        # Cancel old GRID orders only — preserve TP orders placed by reconciliation.
        # TP orders have client_order_id starting with "tp_".
        try:
            open_orders = await self._client.get_open_orders(signal.symbol)
            cancelled = 0
            preserved = 0
            for o in open_orders:
                cid = o.get("clientOrderId", "")
                if cid.startswith("tp_"):
                    preserved += 1
                    continue  # Never cancel take-profit orders
                try:
                    await self._client.cancel_order(
                        signal.symbol, order_id=str(o.get("orderId", ""))
                    )
                    cancelled += 1
                except Exception:
                    pass
            logger.info(
                f"  Rebalance: cancelled {cancelled} grid orders, "
                f"preserved {preserved} TP orders"
            )
        except Exception as e:
            logger.debug(f"  Could not query open orders: {e}")

        tracked_orders = []
        trade_id = metadata.get("grid_id", "")
        self._ledger_register_grid(trade_id, signal.symbol)

        # ---- Unidirectional deployment with existing inventory integration ----
        base_asset = signal.symbol.replace("USDT", "")
        has_base, has_quote = True, True
        base_free = 0.0
        try:
            base_bal = await self._client.get_asset_balance(base_asset)
            quote_bal = await self._client.get_asset_balance("USDT")
            base_free = base_bal.free
            has_base = base_free > 0
            has_quote = quote_bal.free > 0
            if not has_base:
                logger.info(f"  No {base_asset} — skipping SELL levels")
            if not has_quote:
                logger.info(f"  No USDT — skipping BUY levels")
        except Exception:
            pass

        # Distribute existing DOGE across sell levels (pyramid weights).
        # Compute total weight for sell levels to allocate inventory.
        sell_levels_data = [l for l in levels if l.get("side") == "SELL"]
        buy_levels_data = [l for l in levels if l.get("side") == "BUY"]
        sell_total_weight = sum(
            self._pyramid_weight(i, len(sell_levels_data))
            for i in range(len(sell_levels_data))
        ) if sell_levels_data else 1.0
        buy_total_weight = sum(
            self._pyramid_weight(i, len(buy_levels_data))
            for i in range(len(buy_levels_data))
        ) if buy_levels_data else 1.0

        for idx, level_dict in enumerate(levels):
            side = level_dict.get("side", "")
            if side == "SELL" and not has_base:
                continue
            if side == "BUY" and not has_quote:
                continue

            # Allocate existing inventory for sells, capital for buys
            if side == "SELL" and base_free > 0 and sell_levels_data:
                sell_idx = sell_levels_data.index(level_dict)
                weight = self._pyramid_weight(sell_idx, len(sell_levels_data))
                allocated_qty = base_free * weight / sell_total_weight
                # Override grid-computed quantity with actual available inventory
                level_dict = dict(level_dict)
                level_dict["quantity"] = allocated_qty

            try:
                price, qty = self._round_to_tick(signal.symbol, level_dict["price"], level_dict["quantity"])
                if qty <= 0:
                    continue

                # Skip dust orders below exchange minimum notional ($1 for DOGE)
                notional = price * qty
                if notional < 1.0:
                    logger.debug(
                        f"  Skipping dust level: {side} {qty} @ {price} "
                        f"(notional=${notional:.2f} < $1.00)"
                    )
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

                # Update ledger: order is now OPEN on exchange
                self._ledger_order_open(order_id, response.exchange_order_id)

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
    def _pyramid_weight(index: int, total_levels: int, decay: float = 0.5) -> float:
        """Pyramid weight for level index (0 = closest to reference, highest weight)."""
        return decay ** index

    @staticmethod
    def _round_to_tick(symbol: str, price: float, qty: float) -> tuple:
        """Round price and quantity to exchange-allowed tick sizes."""
        # Binance spot tick sizes
        ticks = {
            "BTCUSDT": (2, 5),     # price: 0.01, qty: 5dp
            "ETHUSDT": (2, 4),
            "SOLUSDT": (2, 2),
            "BNBUSDT": (1, 3),
            "DOGEUSDT": (5, 0),   # price: 0.00001, qty: whole DOGE
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
        # Initial delay: let the grid deploy first before reconciliation
        # kicks in, otherwise old fills trigger TP before grid_active is set.
        await asyncio.sleep(15.0)
        while self._running:
            try:
                await self.reconcile()
                await asyncio.sleep(self.reconcile_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Reconciliation error: {e}")

    async def reconcile(self) -> None:
        """
        Sync local order state with exchange — detect fills via myTrades API.
        """
        symbols = set(o.symbol for o in self._orders.values()) or {"DOGEUSDT"}
        for symbol in symbols:
            try:
                # Query recent trades (fills) from exchange
                trades = await self._client._signed_request("GET",
                    f"{self._client._api_prefix}/myTrades",
                    {"symbol": symbol.upper(), "limit": 50},
                )
                processed_ids = set()

                for trade in trades:
                    ex_order_id = str(trade.get("orderId", ""))
                    if not ex_order_id or ex_order_id in processed_ids:
                        continue
                    processed_ids.add(ex_order_id)

                    fill_qty = float(trade.get("qty", 0))
                    fill_price = float(trade.get("price", 0))
                    # Binance myTrades uses isBuyer, not side
                    fill_side = "BUY" if trade.get("isBuyer", False) else "SELL"

                    # Find matching order: first check in-memory, then ledger
                    order_id = None
                    for oid, order in self._orders.items():
                        if order.exchange_order_id == ex_order_id:
                            order_id = oid
                            order.status = "FILLED"
                            order.filled_quantity = fill_qty
                            break

                    # If not in memory, check ledger for the order
                    if order_id is None:
                        try:
                            import sqlite3
                            ledger_path = os.environ.get("DB_PATH", "data/trading_ledger.db")
                            conn = sqlite3.connect(ledger_path)
                            row = conn.execute(
                                "SELECT order_id FROM orders WHERE exchange_order_id=?",
                                (ex_order_id,)
                            ).fetchone()
                            conn.close()
                            if row:
                                order_id = row[0]
                        except Exception:
                            pass

                    if order_id:
                        self._ledger_update_fill(
                            order_id=order_id,
                            exchange_order_id=ex_order_id,
                            filled_quantity=fill_qty,
                            fill_price=fill_price,
                        )
                    logger.debug(f"  Fill: {symbol} {fill_side} {fill_qty} @ ${fill_price:.2f}")

                    # Consecutive buy circuit breaker — only count NEW fills
                    if after_grid:
                        trade_id = str(trade.get("id", ""))
                        if trade_id and trade_id not in self._counted_fills:
                            self._counted_fills.add(trade_id)
                            # Cap set size
                            if len(self._counted_fills) > 500:
                                self._counted_fills = set(list(self._counted_fills)[-250:])
                            if fill_side == "BUY":
                                self._consecutive_buys += 1
                                if self._consecutive_buys >= self._consecutive_buy_limit:
                                    logger.warning(
                                        f"⚠ {self._consecutive_buys} consecutive buys "
                                        f"since grid deploy — possible one-sided market."
                                    )
                            else:
                                self._consecutive_buys = 0

                    # ---- AUTO PLACE TAKE-PROFIT ORDER ----
                    # Only for fills that happened AFTER grid deployment.
                    # 10s buffer: local clock may differ from exchange clock.
                    fill_time_ms = trade.get("time", 0)
                    fill_time = fill_time_ms / 1000.0 if fill_time_ms else 0
                    after_grid = (self.grid_deployed_at > 0
                                  and fill_time > self.grid_deployed_at - 10.0)
                    if fill_side == "BUY" and fill_qty > 0:
                        if not after_grid:
                            logger.debug(
                                f"  TP skipped (before grid deploy): "
                                f"fill_time={fill_time:.0f} < deploy_time={self.grid_deployed_at:.0f}"
                            )
                            continue
                        tp_price = round(fill_price * 1.005, 2)
                        trade_id = str(trade.get("id", ""))
                        tp_tag = f"tp_{trade_id}"

                        # Dedup: never retry the same TP order
                        if not hasattr(self, '_tp_attempted'):
                            self._tp_attempted: set = set()
                        if tp_tag in self._tp_attempted:
                            continue
                        self._tp_attempted.add(tp_tag)
                        if len(self._tp_attempted) > 1000:
                            self._tp_attempted = set(list(self._tp_attempted)[-500:])

                        # Use actual balance (accounts for fees) floored to lot size
                        try:
                            base_asset = symbol.replace("USDT", "")
                            bal = await self._client.get_asset_balance(base_asset)
                            available = bal.free
                            if available <= 0:
                                logger.debug(f"  No {base_asset} to sell for TP")
                                continue
                            info = await self._client.get_exchange_info(symbol)
                            lot_step = 1.0
                            for s in info.get("symbols", []):
                                if s.get("symbol") == symbol.upper():
                                    for f in s.get("filters", []):
                                        if f["filterType"] == "LOT_SIZE":
                                            lot_step = float(f["stepSize"])
                                            break
                                    break
                            sell_qty = (available // lot_step) * lot_step
                            if sell_qty <= 0:
                                logger.debug(f"  TP qty {available} below lot step {lot_step}")
                                continue
                        except Exception:
                            sell_qty = fill_qty

                        try:
                            tp_req = OrderRequest(
                                symbol=symbol, side="SELL", order_type="LIMIT",
                                price=tp_price, quantity=sell_qty,
                                time_in_force="GTC", client_order_id=tp_tag,
                            )
                            tp_resp = await self._client.place_order(tp_req)
                            logger.info(f"  TP placed: SELL {sell_qty} @ ${tp_price:.2f}")
                            self._orders[tp_tag] = TrackedOrder(
                                order_id=tp_tag, exchange_order_id=tp_resp.exchange_order_id,
                                symbol=symbol, side="SELL", order_type="LIMIT",
                                price=tp_price, quantity=sell_qty, status="OPEN",
                            )
                        except Exception as e:
                            logger.error(f"  Failed to place TP: {e}")

                    # ---- AUTO REPLENISH BUY (grid level cycling) ----
                    # When a SELL (TP) fills, place a new BUY at the original
                    # entry price to revive the grid level. This keeps each
                    # level cycling independently without waiting for rebalance.
                    if fill_side == "SELL" and fill_qty > 0 and after_grid:
                        client_oid = str(trade.get("orderId", ""))
                        rebuy_tag = f"rebuy_{client_oid}"

                        if not hasattr(self, '_rebuy_attempted'):
                            self._rebuy_attempted: set = set()
                        if rebuy_tag in self._rebuy_attempted:
                            continue
                        self._rebuy_attempted.add(rebuy_tag)
                        if len(self._rebuy_attempted) > 1000:
                            self._rebuy_attempted = set(list(self._rebuy_attempted)[-500:])

                        # Compute entry price from TP price
                        entry_price = round(fill_price / 1.005, 5)
                        # Use actual USDT balance to determine buy qty, capped at fill_qty
                        try:
                            usdt_bal = await self._client.get_asset_balance("USDT")
                            info = await self._client.get_exchange_info(symbol)
                            lot_step = 1.0
                            for s in info.get("symbols", []):
                                if s.get("symbol") == symbol.upper():
                                    for f in s.get("filters", []):
                                        if f["filterType"] == "LOT_SIZE":
                                            lot_step = float(f["stepSize"])
                                            break
                                    break
                            max_qty = min(fill_qty, usdt_bal.free / entry_price * 0.98)  # 2% margin
                            rebuy_qty = (max_qty // lot_step) * lot_step
                            if rebuy_qty <= 0 or rebuy_qty * entry_price < 1.0:
                                continue
                        except Exception:
                            rebuy_qty = fill_qty

                        try:
                            rebuy_req = OrderRequest(
                                symbol=symbol, side="BUY", order_type="LIMIT",
                                price=entry_price, quantity=rebuy_qty,
                                time_in_force="GTC", client_order_id=rebuy_tag,
                            )
                            rebuy_resp = await self._client.place_order(rebuy_req)
                            logger.info(f"  ↻ Grid cycle: SELL filled → BUY {rebuy_qty} @ ${entry_price:.5f}")
                            self._orders[rebuy_tag] = TrackedOrder(
                                order_id=rebuy_tag, exchange_order_id=rebuy_resp.exchange_order_id,
                                symbol=symbol, side="BUY", order_type="LIMIT",
                                price=entry_price, quantity=rebuy_qty, status="OPEN",
                            )
                        except Exception as e:
                            logger.debug(f"  Rebuy skipped: {e}")

            except Exception as e:
                logger.debug(f"Reconciliation skipped for {symbol}: {e}")


from collections import defaultdict
