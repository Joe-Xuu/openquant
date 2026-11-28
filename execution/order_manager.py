"""
================================================================================
ORDER MANAGER — Signal-to-Exchange Dispatch, User Data Stream & Reconciliation
================================================================================

Translates StrategySignals into exchange orders and tracks their lifecycle
from PENDING → OPEN → FILLED/CANCELLED/REJECTED.

KEY GUARANTEES:
    - Prefix isolation: Grid orders (grid_/rebuy_/tp_) and Trend orders
      (trend_/trend_sl_) never cancel each other.
    - Dual-path fill detection: Real-time Binance User Data Stream WebSocket
      (executionReport) + low-frequency REST /myTrades fallback reconciliation,
      both converging on the same idempotent _handle_fill_event() pipeline.
    - Partial-fill safe: Deduped by exchange trade_id (never dropping 2nd
      partial fill of an order), and TP sell qty is strictly capped at
      min(available_base, net_fill_qty).
    - Exchange precision via cached SymbolInfo (no hardcoded symbol tables).
================================================================================
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from collections import OrderedDict, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

import aiohttp

from execution.exchange_client import ExchangeClient, OrderRequest, OrderResponse, SymbolInfo
from strategy.signal import SignalAction, StrategySignal

logger = logging.getLogger("execution.order_manager")

# ClientOrderId prefixes for strict engine isolation
GRID_BUY_PREFIXES = ("grid_", "co_", "rebuy_")
GRID_TP_PREFIX = "tp_"
TREND_PREFIXES = ("trend_", "trend_sl_")


# ---------------------------------------------------------------------------
# Bounded FIFO Dedup Set
# ---------------------------------------------------------------------------

class BoundedDedupSet:
    """Insertion-ordered bounded set with deterministic FIFO eviction."""

    def __init__(self, max_size: int = 2000):
        self._max_size = max_size
        self._data: OrderedDict[str, None] = OrderedDict()

    def add(self, key: str) -> None:
        if key in self._data:
            self._data.move_to_end(key)
            return
        self._data[key] = None
        while len(self._data) > self._max_size:
            self._data.popitem(last=False)

    def discard(self, key: str) -> None:
        self._data.pop(key, None)

    def update(self, keys) -> None:
        for k in keys:
            self.add(k)

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def __len__(self) -> int:
        return len(self._data)


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


@dataclass
class FillEvent:
    """Normalized trade execution event (from either WS executionReport or REST myTrades)."""
    trade_id: str
    exchange_order_id: str
    client_order_id: str
    symbol: str
    side: str  # BUY or SELL
    price: float
    qty: float
    commission: float
    commission_asset: str
    timestamp_sec: float


# ---------------------------------------------------------------------------
# OrderManager
# ---------------------------------------------------------------------------

class OrderManager:
    """
    Manages the full lifecycle of exchange orders with real-time WS fill
    streaming and periodic REST fallback reconciliation.
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
        reconcile_interval: float = 60.0,
        grid_profit_pct: float = 1.0,
        enable_user_stream: bool = True,
        symbols: Optional[List[str]] = None,
        # Grid fill persistence callbacks
        ledger_grid_buy_fill: Callable = None,
        ledger_grid_sell_placed: Callable = None,
        ledger_grid_sell_filled: Callable = None,
        ledger_grid_reset_sell_placed: Callable = None,
        ledger_get_pending_tp_fills: Callable = None,
        ledger_get_tp_attempted_tags: Callable = None,
        ledger_get_grid_fill_by_sell_order: Callable = None,
        ledger_get_order_id_by_exchange: Callable = None,
        # Optional callback when an exchange-side trend stop-loss fills
        on_trend_stop_filled: Optional[Callable[[str, float, float], None]] = None,
    ):
        self._client = exchange_client
        self._ledger_record_order = ledger_record_order
        self._ledger_order_open = ledger_order_open or (lambda oid, eid: None)
        self._ledger_update_fill = ledger_update_fill or (lambda *a, **kw: None)
        self._ledger_record_trade_open = ledger_record_trade_open or (lambda *a, **kw: "")
        self._ledger_record_trade_close = ledger_record_trade_close or (lambda *a, **kw: (0, 0))
        self._ledger_register_grid = ledger_register_grid or (lambda tid, sym: tid)
        self.reconcile_interval = reconcile_interval
        self.enable_user_stream = enable_user_stream
        self._symbols: List[str] = [s.upper() for s in (symbols or ["DOGEUSDT"])]
        self._on_trend_stop_filled = on_trend_stop_filled

        self.grid_active = False
        self.grid_deployed_at: float = 0.0
        self.grid_profit_pct = grid_profit_pct
        self._grid_profit_mult = 1.0 + grid_profit_pct / 100.0
        self._consecutive_buys: int = 0
        self._consecutive_buy_limit: int = 5

        # Grid fill persistence callbacks (DB-backed, survive restarts)
        self._ledger_grid_buy_fill = ledger_grid_buy_fill or (lambda *a, **kw: False)
        self._ledger_grid_sell_placed = ledger_grid_sell_placed or (lambda *a, **kw: None)
        self._ledger_grid_sell_filled = ledger_grid_sell_filled or (lambda *a, **kw: None)
        self._ledger_grid_reset_sell_placed = ledger_grid_reset_sell_placed or (lambda *a, **kw: None)
        self._ledger_get_pending_tp_fills = ledger_get_pending_tp_fills or (lambda *a, **kw: [])
        self._ledger_get_tp_attempted_tags = ledger_get_tp_attempted_tags or (lambda *a, **kw: [])
        self._ledger_get_grid_fill_by_sell_order = ledger_get_grid_fill_by_sell_order or (lambda *a, **kw: None)
        self._ledger_get_order_id_by_exchange = ledger_get_order_id_by_exchange or (lambda *a, **kw: None)

        # Deterministic FIFO bounded dedup caches (seeded from DB on startup)
        self._counted_fills = BoundedDedupSet(max_size=2000)
        self._tp_attempted = BoundedDedupSet(max_size=2000)
        self._rebuy_attempted = BoundedDedupSet(max_size=2000)

        # Tracked orders: internal_order_id -> TrackedOrder
        self._orders: Dict[str, TrackedOrder] = {}
        self._active_trend_trade_id: Dict[str, str] = {}
        self._dispatch_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._fill_lock = asyncio.Lock()

        # Background tasks
        self._reconcile_task: Optional[asyncio.Task] = None
        self._user_stream_task: Optional[asyncio.Task] = None
        self._keepalive_task: Optional[asyncio.Task] = None
        self._listen_key: Optional[str] = None
        self._running = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the order manager, user data stream WS, and fallback reconciliation loop."""
        self._running = True
        await self._seed_tp_cache()
        self._reconcile_task = asyncio.create_task(self._reconciliation_loop())
        if self.enable_user_stream and getattr(self._client, "api_key", ""):
            self._user_stream_task = asyncio.create_task(self._user_stream_loop())
            self._keepalive_task = asyncio.create_task(self._listen_key_keepalive_loop())
        logger.info(
            f"OrderManager started (user_stream={self.enable_user_stream}, "
            f"reconcile_interval={self.reconcile_interval}s)"
        )

    async def _seed_tp_cache(self) -> None:
        """Load persistent TP state into in-memory dedup cache."""
        try:
            tags = self._ledger_get_tp_attempted_tags()
            self._tp_attempted = BoundedDedupSet(max_size=2000)
            self._tp_attempted.update(tags)
            if len(self._tp_attempted):
                logger.info(f"TP cache seeded: {len(self._tp_attempted)} tags from DB")
        except Exception as e:
            logger.warning(f"Could not seed TP cache from DB: {e}")

    async def stop(self) -> None:
        """Stop all background tasks and close listenKey."""
        self._running = False
        for task in (self._reconcile_task, self._user_stream_task, self._keepalive_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        if self._listen_key:
            await self._client.close_listen_key(self._listen_key)
            self._listen_key = None
        logger.info("OrderManager stopped")

    # ------------------------------------------------------------------
    # Precision Helpers (SymbolInfo-backed)
    # ------------------------------------------------------------------

    def _get_symbol_info(self, symbol: str) -> SymbolInfo:
        if hasattr(self._client, "get_symbol_info"):
            return self._client.get_symbol_info(symbol)
        return SymbolInfo.default_for(symbol)

    def _round_to_tick(self, symbol: str, price: float, qty: float) -> Tuple[float, float]:
        """Round price to tick_size and floor qty to step_size using SymbolInfo."""
        info = self._get_symbol_info(symbol)
        return info.round_price(price), info.floor_qty(qty)

    # ------------------------------------------------------------------
    # Selective Order Cancellation (Prefix Isolation)
    # ------------------------------------------------------------------

    async def _cancel_orders_by_prefix(
        self, symbol: str, prefixes: Tuple[str, ...]
    ) -> Tuple[int, int]:
        """
        Cancel ONLY open orders whose clientOrderId starts with one of `prefixes`.
        Returns (cancelled_count, preserved_count).
        """
        open_orders = await self._client.get_open_orders(symbol)
        cancelled, preserved = 0, 0
        for o in open_orders:
            cid = o.get("clientOrderId", "")
            if any(cid.startswith(p) for p in prefixes):
                try:
                    await self._client.cancel_order(
                        symbol, order_id=str(o.get("orderId", ""))
                    )
                    cancelled += 1
                except Exception as e:
                    logger.debug(f"Could not cancel {symbol} {cid}: {e}")
            else:
                preserved += 1
        return cancelled, preserved

    # ------------------------------------------------------------------
    # Startup TP Recovery
    # ------------------------------------------------------------------

    async def sync_tp_orders(self) -> int:
        """
        Verify all TP orders marked as 'placed' actually exist on the exchange.

        For each grid_fills row with sell_placed=TRUE, sell_filled=FALSE:
          - If FILLED on exchange → mark sell_filled=TRUE
          - If CANCELED/EXPIRED/REJECTED/missing → reset sell_placed=FALSE AND
            remove from _tp_attempted so recover_pending_tp_fills() re-places it.
        """
        rows = self._ledger_get_pending_tp_fills()
        to_check = [r for r in rows if r.get("sell_placed") and r.get("sell_order_id")]
        if not to_check:
            return 0

        corrected = 0
        for fill in to_check:
            trade_id = fill["trade_id"]
            symbol = fill["symbol"]
            sell_oid = fill["sell_order_id"]
            tp_tag = f"{GRID_TP_PREFIX}{trade_id}"
            try:
                order = await self._client.get_order_status(symbol, order_id=sell_oid)
                status = order.get("status", "")
                if status == "FILLED":
                    fill_price = float(order.get("price", 0))
                    fill_time = datetime.fromtimestamp(
                        order.get("updateTime", 0) / 1000.0, tz=timezone.utc
                    ).isoformat()
                    self._ledger_grid_sell_filled(trade_id, fill_price, fill_time)
                    logger.info(
                        f"  TP sync: {symbol} sell {sell_oid} was FILLED @ {fill_price:.5f}"
                    )
                    corrected += 1
                elif status in ("CANCELED", "EXPIRED", "REJECTED"):
                    self._ledger_grid_reset_sell_placed(trade_id)
                    self._tp_attempted.discard(tp_tag)
                    logger.warning(
                        f"  TP sync: {symbol} sell {sell_oid} is {status} — "
                        f"resetting sell_placed for recovery"
                    )
                    corrected += 1
            except Exception as e:
                err = str(e)
                if "Order does not exist" in err or "-2013" in err:
                    self._ledger_grid_reset_sell_placed(trade_id)
                    self._tp_attempted.discard(tp_tag)
                    logger.warning(
                        f"  TP sync: {symbol} sell {sell_oid} missing on exchange — "
                        f"resetting sell_placed"
                    )
                    corrected += 1
                else:
                    logger.debug(f"  TP sync: could not check {symbol} {sell_oid}: {e}")

        if corrected:
            logger.info(f"  TP sync complete: corrected {corrected} stale flags")
        return corrected

    async def reconcile_futures_stale_grid_fills(
        self, symbol: str, open_long_qty: float, mark_price: float = 0.0
    ) -> int:
        """
        In Futures mode, if exchange holds no open LONG position (flat or net SHORT),
        any pending grid_fills (sell_filled=FALSE) are stale ghosts whose reduceOnly
        TP sells expired when the position was closed or flipped. Mark them settled so
        they stop eating grid budget and cannot spawn phantom TPs later.
        """
        if open_long_qty > 0:
            return 0
        pending = self._ledger_get_pending_tp_fills(symbol)
        if not pending:
            return 0

        now_iso = datetime.now(timezone.utc).isoformat()
        cleared = 0
        for fill in pending:
            tid = fill.get("trade_id")
            if not tid:
                continue
            settle_px = mark_price if mark_price > 0 else float(fill.get("buy_price", 0.0))
            try:
                self._ledger_grid_sell_filled(tid, settle_px, now_iso)
                self._tp_attempted.discard(f"{GRID_TP_PREFIX}{tid}")
                cleared += 1
            except Exception as e:
                logger.debug(f"Could not clear stale grid fill {tid}: {e}")

        if cleared:
            logger.warning(
                f"  Cleared {cleared} stale {symbol} grid_fills (Futures open_long_qty=0)"
            )
        return cleared

    async def recover_pending_tp_fills(self) -> int:
        """
        Recover TP orders for BUY fills where sell_placed=FALSE.
        """
        pending = [r for r in self._ledger_get_pending_tp_fills() if not r.get("sell_placed")]
        if not pending:
            logger.info("  No pending TP fills to recover")
            return 0

        recovered = 0
        for fill in pending:
            trade_id = fill["trade_id"]
            symbol = fill["symbol"]
            sell_price = float(fill["sell_price"])
            buy_qty = float(fill["buy_qty"])
            buy_price_float = float(fill.get("buy_price", 0))
            tp_tag = f"{GRID_TP_PREFIX}{trade_id}"

            if tp_tag in self._tp_attempted:
                continue

            if sell_price <= buy_price_float:
                logger.warning(
                    f"  Recovery TP skipped: TP {sell_price:.5f} <= buy {buy_price_float:.5f} "
                    f"for {symbol} trade {trade_id}"
                )
                self._tp_attempted.add(tp_tag)
                continue

            sym_info = self._get_symbol_info(symbol)
            is_futures = getattr(self._client, "market", "spot") == "futures"

            # Cap sell_qty at min(available_long, buy_qty) floored to step_size
            try:
                available = await self._get_available_long_qty(symbol, sym_info)
                if available <= 0:
                    logger.debug(f"  Recovery: no {sym_info.base_asset} long/free for trade {trade_id}")
                    continue
                sell_qty = sym_info.floor_qty(min(available, buy_qty))
            except Exception:
                sell_qty = sym_info.floor_qty(buy_qty)

            sell_price = sym_info.round_price(sell_price)
            if not sym_info.is_valid_order(sell_price, sell_qty):
                logger.debug(
                    f"  Recovery TP invalid/dust: qty={sell_qty} @ {sell_price} for {symbol}"
                )
                self._tp_attempted.add(tp_tag)
                continue

            # If market moved above original TP, place 0.1% above market to capture extra profit
            try:
                current_px = await self._client.get_ticker_price(symbol)
                if current_px > sell_price:
                    adjusted = sym_info.round_price(current_px * 1.001)
                    if adjusted > sell_price:
                        logger.info(
                            f"  TP adjusted: {sell_price:.5f} → {adjusted:.5f} "
                            f"(market @ {current_px:.5f})"
                        )
                        sell_price = adjusted
            except Exception:
                pass

            try:
                tp_req = OrderRequest(
                    symbol=symbol, side="SELL", order_type="LIMIT",
                    price=sell_price, quantity=sell_qty,
                    time_in_force="GTC", client_order_id=tp_tag,
                    reduce_only=is_futures,
                )
                tp_resp = await self._client.place_order(tp_req)
                self._tp_attempted.add(tp_tag)
                self._ledger_grid_sell_placed(trade_id, tp_resp.exchange_order_id, tp_tag)
                self._orders[tp_tag] = TrackedOrder(
                    order_id=tp_tag, exchange_order_id=tp_resp.exchange_order_id,
                    client_order_id=tp_tag,
                    symbol=symbol, side="SELL", order_type="LIMIT",
                    price=sell_price, quantity=sell_qty, status="OPEN",
                )
                recovered += 1
                logger.info(
                    f"  TP RECOVERED: SELL {sell_qty} @ ${sell_price:.5f} for {symbol} "
                    f"(trade {trade_id})"
                )
            except Exception as e:
                # Check if order already exists on exchange (crashed before DB update)
                try:
                    existing = await self._client.get_order_status(symbol, client_order_id=tp_tag)
                    if existing and existing.get("status") in ("NEW", "PARTIALLY_FILLED"):
                        self._ledger_grid_sell_placed(trade_id, str(existing["orderId"]), tp_tag)
                        self._tp_attempted.add(tp_tag)
                        recovered += 1
                        logger.info(f"  TP RECOVERED (existing): SELL @ ${sell_price:.5f}")
                        continue
                except Exception:
                    pass
                logger.error(f"  Recovery TP failed for {symbol} trade {trade_id}: {e}")
                self._tp_attempted.add(tp_tag)

        if recovered:
            logger.info(f"  TP recovery complete: placed/recovered {recovered} orders")
        return recovered

    # ------------------------------------------------------------------
    # Signal Dispatch
    # ------------------------------------------------------------------

    async def dispatch_signal(
        self,
        signal: StrategySignal,
        risk_params: Optional[Dict[str, Any]] = None,
    ) -> List[TrackedOrder]:
        """Dispatch an approved StrategySignal to the exchange."""
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
    # Grid Dispatch (Prefix: grid_)
    # ------------------------------------------------------------------

    async def _dispatch_grid(
        self, signal: StrategySignal, metadata: Dict[str, Any]
    ) -> List[TrackedOrder]:
        """
        Deploy grid BUY orders only (Spot mode).
        Cancels only existing grid_/co_/rebuy_ orders; preserves tp_*, trend_*, and manual orders.
        """
        levels = metadata.get("levels", [])
        if not levels:
            logger.warning("Grid signal has no levels")
            return []

        try:
            cancelled, preserved = await self._cancel_orders_by_prefix(
                signal.symbol, GRID_BUY_PREFIXES
            )
            logger.info(
                f"  Rebalance: cancelled {cancelled} grid orders, "
                f"preserved {preserved} (TP + trend + manual)"
            )
        except Exception as e:
            logger.debug(f"  Could not query open orders: {e}")

        tracked_orders: List[TrackedOrder] = []
        trade_id = metadata.get("grid_id", "")
        self._ledger_register_grid(trade_id, signal.symbol)

        sym_info = self._get_symbol_info(signal.symbol)

        try:
            quote_bal = await self._client.get_asset_balance(sym_info.quote_asset)
            available_quote = quote_bal.free
            if available_quote <= 0:
                logger.warning(f"  No {sym_info.quote_asset} balance — skipping grid deployment")
                return []
        except Exception:
            available_quote = float("inf")

        buy_levels = [l for l in levels if l.get("side") == "BUY"]
        remaining_quote = available_quote

        for level_dict in buy_levels:
            try:
                price, qty = self._round_to_tick(
                    signal.symbol, level_dict["price"], level_dict["quantity"]
                )
                if not sym_info.is_valid_order(price, qty):
                    logger.debug(
                        f"  Skipping dust/invalid level: qty={qty} @ {price} "
                        f"(min_notional={sym_info.min_notional})"
                    )
                    continue

                notional = price * qty
                if notional > remaining_quote:
                    logger.debug(
                        f"  Skipping level (insufficient free {sym_info.quote_asset}: "
                        f"need ${notional:.2f}, left ${remaining_quote:.2f})"
                    )
                    continue

                client_oid = f"grid_{uuid.uuid4().hex[:16]}"
                order_req = OrderRequest(
                    symbol=signal.symbol,
                    side="BUY",
                    order_type="LIMIT",
                    price=price,
                    quantity=qty,
                    time_in_force="GTC",
                    client_order_id=client_oid,
                )

                order_id = self._ledger_record_order(
                    trade_id=trade_id, symbol=signal.symbol,
                    side="BUY", order_type="LIMIT",
                    quantity=qty, price=price,
                    client_order_id=client_oid,
                )

                response = await self._client.place_order(order_req)
                self._ledger_order_open(order_id, response.exchange_order_id)
                remaining_quote -= notional

                tracked = TrackedOrder(
                    order_id=order_id,
                    exchange_order_id=response.exchange_order_id,
                    client_order_id=response.client_order_id,
                    symbol=signal.symbol, side="BUY", order_type="LIMIT",
                    price=price, quantity=qty,
                    status=response.status, trade_id=trade_id,
                    grid_id=metadata.get("grid_id"),
                )
                self._orders[order_id] = tracked
                tracked_orders.append(tracked)
                logger.info(
                    f"Grid BUY placed: {qty} @ {price} [{response.exchange_order_id}]"
                )

            except Exception as e:
                logger.error(f"Failed to place grid BUY: {e}")
                continue

        return tracked_orders

    async def _cancel_grid(
        self, signal: StrategySignal, metadata: Dict[str, Any]
    ) -> List[TrackedOrder]:
        """Cancel open grid orders for the symbol (preserving trend/manual orders).

        When ``metadata.get("liquidate_inventory")`` is True (e.g. macro-bear exit),
        also cancels ``tp_*`` sell orders and market-sells remaining pending grid
        inventory (capped by free base balance).
        """
        grid_id = metadata.get("grid_id", "")
        liquidate = bool(metadata.get("liquidate_inventory", False))
        cancelled_orders: List[TrackedOrder] = []

        prefixes = GRID_BUY_PREFIXES + (("tp_",) if liquidate else ())
        try:
            await self._cancel_orders_by_prefix(signal.symbol, prefixes)
        except Exception as e:
            logger.error(f"Failed to cancel grid orders for {signal.symbol}: {e}")

        for order in self._orders.values():
            if order.grid_id == grid_id and order.status == "OPEN":
                order.status = "CANCELLED"
                cancelled_orders.append(order)

        if liquidate:
            try:
                pending_fills = self._ledger_get_pending_tp_fills(signal.symbol)
                pending_qty = sum(float(f.get("buy_qty", 0.0)) for f in pending_fills)
                if pending_qty > 0:
                    sym_info = self._get_symbol_info(signal.symbol)
                    is_futures = getattr(self._client, "market", "spot") == "futures"
                    available = await self._get_available_long_qty(signal.symbol, sym_info)
                    sell_qty = sym_info.floor_qty(min(pending_qty, available))
                    px = await self._client.get_ticker_price(signal.symbol)
                    if sym_info.is_valid_order(px, sell_qty):
                        exit_cid = f"grid_exit_{uuid.uuid4().hex[:14]}"
                        resp = await self._client.place_order(
                            OrderRequest(
                                symbol=signal.symbol,
                                side="SELL",
                                order_type="MARKET",
                                quantity=sell_qty,
                                client_order_id=exit_cid,
                                reduce_only=is_futures,
                            )
                        )
                        fill_px = (
                            (resp.cummulative_quote_qty / resp.executed_qty)
                            if (resp.executed_qty > 0 and resp.cummulative_quote_qty > 0)
                            else (resp.price or px)
                        )
                        now_dt = datetime.now(timezone.utc)
                        for f in pending_fills:
                            tid = f.get("trade_id")
                            if tid:
                                self._ledger_grid_sell_filled(tid, fill_px, now_dt)
                        logger.warning(
                            f"Liquidated {sell_qty} {sym_info.base_asset} grid inventory "
                            f"@ {fill_px:.5f} (reason={metadata.get('reason', 'stop_grid')})"
                        )
            except Exception as e:
                logger.error(f"Failed to liquidate grid inventory for {signal.symbol}: {e}")

        self.grid_active = False
        try:
            if grid_id:
                self._ledger_record_trade_close(trade_id=grid_id, exit_price=0.0, fee=0.0)
        except Exception:
            pass

        return cancelled_orders

    # ------------------------------------------------------------------
    # Trend Dispatch (Prefixes: trend_, trend_sl_)
    # ------------------------------------------------------------------

    async def _dispatch_trend(
        self, signal: StrategySignal, metadata: Dict[str, Any]
    ) -> List[TrackedOrder]:
        """Place a trend entry order with exchange-side STOP_LOSS_LIMIT protection."""
        direction = metadata["direction"]
        entry_price = metadata.get("entry_price", 0.0)
        raw_size = metadata.get("position_size", 0.0)
        raw_stop = metadata.get("stop_loss", 0.0)

        sym_info = self._get_symbol_info(signal.symbol)
        entry_px, position_size = self._round_to_tick(signal.symbol, entry_price, raw_size)
        if not sym_info.is_valid_order(entry_px, position_size):
            logger.warning(
                f"Trend entry skipped (below exchange minimums): "
                f"qty={position_size} @ {entry_px}"
            )
            return []

        side = "BUY" if direction == "LONG" else "SELL"
        trade_id = f"trend_{signal.symbol}_{int(datetime.now(timezone.utc).timestamp())}"
        client_oid = f"trend_{uuid.uuid4().hex[:16]}"

        self._ledger_record_trade_open(
            trade_id=trade_id,
            symbol=signal.symbol,
            side=side,
            quantity=position_size,
            price=entry_px,
        )

        entry_order_req = OrderRequest(
            symbol=signal.symbol,
            side=side,
            order_type="MARKET",
            quantity=position_size,
            client_order_id=client_oid,
        )

        order_id = self._ledger_record_order(
            trade_id=trade_id,
            symbol=signal.symbol,
            side=side,
            order_type="MARKET",
            quantity=position_size,
            client_order_id=client_oid,
        )

        tracked_orders: List[TrackedOrder] = []
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
                filled_quantity=response.executed_qty,
                status=response.status,
                trade_id=trade_id,
            )
            self._orders[order_id] = tracked
            tracked_orders.append(tracked)
            self._active_trend_trade_id[signal.symbol] = trade_id

            exec_qty = response.executed_qty
            cum_quote = response.cummulative_quote_qty
            if exec_qty <= 0 and response.status in ("NEW", "PARTIALLY_FILLED"):
                try:
                    await asyncio.sleep(0.25)
                    st = await self._client.get_order_status(
                        signal.symbol, order_id=response.exchange_order_id
                    )
                    exec_qty = float(st.get("executedQty", 0) or 0)
                    cum_quote = float(st.get("cumQuote") or st.get("cummulativeQuoteQty") or 0)
                    if exec_qty > 0:
                        tracked.filled_quantity = exec_qty
                        tracked.status = str(st.get("status", tracked.status))
                except Exception as poll_err:
                    logger.debug(f"Post-entry order status poll skipped: {poll_err}")

            if exec_qty > 0:
                fill_price = (cum_quote / exec_qty) if cum_quote > 0 else entry_px
                self._ledger_update_fill(
                    order_id=order_id,
                    exchange_order_id=response.exchange_order_id,
                    filled_quantity=exec_qty,
                    fill_price=fill_price,
                )

            stop_qty = exec_qty if exec_qty > 0 else (
                position_size
                if getattr(self._client, "market", "spot") == "futures"
                and response.status in ("NEW", "PARTIALLY_FILLED", "FILLED")
                else 0.0
            )
            if raw_stop > 0 and stop_qty > 0:
                try:
                    await self._place_trend_stop_order(
                        symbol=signal.symbol,
                        direction=direction,
                        stop_price=raw_stop,
                        quantity=stop_qty,
                        trade_id=trade_id,
                    )
                except Exception as sl_err:
                    logger.error(f"Failed to place trend stop-loss for {signal.symbol}: {sl_err}")

            logger.info(f"Trend entry placed: {signal.symbol} {direction} qty={position_size}")

        except Exception as e:
            logger.error(f"Failed to place trend entry: {e}")

        return tracked_orders

    async def _place_trend_stop_order(
        self,
        symbol: str,
        direction: str,
        stop_price: float,
        quantity: float,
        trade_id: str = "",
    ) -> Optional[TrackedOrder]:
        """Place a prefix-tagged stop-loss order (Spot STOP_LOSS_LIMIT / Futures STOP_MARKET reduceOnly)."""
        sym_info = self._get_symbol_info(symbol)
        is_futures = getattr(self._client, "market", "spot") == "futures"
        stop_side = "SELL" if direction == "LONG" else "BUY"
        stop_px = sym_info.round_price(stop_price)
        limit_px = sym_info.round_price(
            stop_px * (0.998 if stop_side == "SELL" else 1.002)
        )
        # Floor quantity to step_size; cap at free base balance only on Spot SELL
        qty = sym_info.floor_qty(quantity)
        if not is_futures and stop_side == "SELL":
            try:
                bal = await self._client.get_asset_balance(sym_info.base_asset)
                qty = sym_info.floor_qty(min(qty, bal.free))
            except Exception:
                pass

        check_px = stop_px if is_futures else limit_px
        if not sym_info.is_valid_order(check_px, qty):
            logger.warning(f"Trend stop-loss skipped (invalid/dust): qty={qty} @ {check_px}")
            return None

        sl_cid = f"trend_sl_{uuid.uuid4().hex[:14]}"
        if is_futures:
            stop_req = OrderRequest(
                symbol=symbol,
                side=stop_side,
                order_type="STOP_MARKET",
                stop_price=stop_px,
                quantity=qty,
                client_order_id=sl_cid,
                reduce_only=True,
            )
        else:
            stop_req = OrderRequest(
                symbol=symbol,
                side=stop_side,
                order_type="STOP_LOSS_LIMIT",
                price=limit_px,
                stop_price=stop_px,
                quantity=qty,
                time_in_force="GTC",
                client_order_id=sl_cid,
            )
        resp = await self._client.place_order(stop_req)
        tracked = TrackedOrder(
            order_id=sl_cid,
            exchange_order_id=resp.exchange_order_id,
            client_order_id=sl_cid,
            symbol=symbol,
            side=stop_side,
            order_type=stop_req.order_type,
            price=stop_px if is_futures else limit_px,
            quantity=qty,
            status=resp.status,
            trade_id=trade_id,
        )
        self._orders[sl_cid] = tracked
        logger.info(
            f"Trend stop-loss placed: {symbol} {stop_side} {stop_req.order_type} "
            f"{qty} stop={stop_px}" + ("" if is_futures else f" limit={limit_px}")
        )
        return tracked

    async def _close_trend(
        self, signal: StrategySignal, metadata: Dict[str, Any]
    ) -> List[TrackedOrder]:
        """Close a trend position at market WITHOUT cancelling grid or TP orders."""
        is_futures = getattr(self._client, "market", "spot") == "futures"
        direction = metadata.get("direction", "LONG")
        close_side = "SELL" if direction == "LONG" else "BUY"
        raw_qty = metadata.get("position_size", 0.0)

        # Cancel ONLY trend_* / trend_sl_* orders — never touch grid or TP orders!
        try:
            cancelled, preserved = await self._cancel_orders_by_prefix(
                signal.symbol, TREND_PREFIXES
            )
            logger.info(
                f"Trend exit cancel: cancelled {cancelled} trend orders, "
                f"preserved {preserved} (grid + TP + manual)"
            )
        except Exception as e:
            logger.warning(f"Could not cancel trend stop orders: {e}")

        sym_info = self._get_symbol_info(signal.symbol)
        qty = sym_info.floor_qty(raw_qty)
        if not is_futures and close_side == "SELL":
            try:
                bal = await self._client.get_asset_balance(sym_info.base_asset)
                qty = sym_info.floor_qty(min(qty, bal.free))
            except Exception:
                pass

        if qty <= 0:
            logger.warning(f"Trend exit skipped for {signal.symbol}: qty={qty}")
            return []

        exit_cid = f"trend_{uuid.uuid4().hex[:16]}"
        order_req = OrderRequest(
            symbol=signal.symbol,
            side=close_side,
            order_type="MARKET",
            quantity=qty,
            client_order_id=exit_cid,
            reduce_only=is_futures,
        )

        tracked_orders: List[TrackedOrder] = []
        try:
            response = await self._client.place_order(order_req)
            logger.info(f"Trend exit placed: {signal.symbol} {close_side} qty={qty}")
            fill_price = (
                response.cummulative_quote_qty / response.executed_qty
                if response.executed_qty > 0 and response.cummulative_quote_qty > 0
                else metadata.get("exit_price", 0.0)
            )
            active_tid = self._active_trend_trade_id.pop(signal.symbol, None)
            if active_tid:
                try:
                    self._ledger_record_trade_close(trade_id=active_tid, exit_price=fill_price)
                except Exception:
                    pass
            tracked_orders.append(TrackedOrder(
                order_id=exit_cid,
                exchange_order_id=response.exchange_order_id,
                client_order_id=exit_cid,
                symbol=signal.symbol,
                side=close_side,
                order_type="MARKET",
                quantity=qty,
                filled_quantity=response.executed_qty,
                status=response.status,
            ))
        except Exception as e:
            logger.error(f"Failed to place trend exit: {e}")

        return tracked_orders

    async def _close_all(self, symbol: str) -> List[TrackedOrder]:
        """Emergency: cancel all orders for a symbol."""
        logger.warning(f"EMERGENCY CLOSE ALL: {symbol}")
        try:
            await self._client.cancel_all_orders(symbol)
        except Exception as e:
            logger.error(f"Failed to cancel all orders: {e}")
        self.grid_active = False
        return []

    async def _modify_position(
        self, signal: StrategySignal, metadata: Dict[str, Any]
    ) -> List[TrackedOrder]:
        """Update trend trailing stop WITHOUT cancelling grid or TP orders."""
        action_type = metadata.get("action_type", "")
        if action_type == "update_trailing_stop":
            try:
                await self._cancel_orders_by_prefix(signal.symbol, ("trend_sl_",))
            except Exception as e:
                logger.warning(f"Could not cancel old trend_sl_ orders: {e}")

            new_stop = metadata.get("new_trailing_stop", 0.0)
            qty = metadata.get("position_size", 0.0)
            if new_stop > 0 and qty > 0:
                direction = metadata.get("direction", "LONG")
                tracked = await self._place_trend_stop_order(
                    symbol=signal.symbol,
                    direction=direction,
                    stop_price=new_stop,
                    quantity=qty,
                    trade_id=self._active_trend_trade_id.get(signal.symbol, ""),
                )
                return [tracked] if tracked else []
        return []

    # ------------------------------------------------------------------
    # Real-Time User Data Stream WebSocket + Keepalive
    # ------------------------------------------------------------------

    async def _user_stream_loop(self) -> None:
        """Listen to Binance User Data Stream for real-time executionReport fills."""
        backoff = 5.0
        while self._running:
            try:
                self._listen_key = await self._client.create_listen_key()
                ws_url = self._client.get_user_stream_ws_url(self._listen_key)
                logger.info("User Data Stream WebSocket connecting...")
                async with self._client._session.ws_connect(ws_url, heartbeat=30.0) as ws:
                    logger.info("User Data Stream WebSocket connected")
                    backoff = 5.0
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            payload = json.loads(msg.data)
                            await self._handle_user_stream_message(payload)
                        elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                            break
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"User Data Stream disconnected: {e}. Reconnecting in {backoff:.0f}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 60.0)

    async def _listen_key_keepalive_loop(self) -> None:
        """Keepalive the Binance listenKey every 25 minutes (expires at 60m)."""
        while self._running:
            try:
                await asyncio.sleep(25 * 60)
                if self._listen_key:
                    await self._client.keepalive_listen_key(self._listen_key)
                    logger.debug("User Data Stream listenKey renewed")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"listenKey keepalive failed: {e}")

    async def _handle_user_stream_message(self, msg: Dict[str, Any]) -> None:
        """Parse Spot executionReport or Futures ORDER_TRADE_UPDATE and route TRADE fills."""
        event_type = msg.get("e")
        if event_type == "executionReport":
            payload = msg
        elif event_type == "ORDER_TRADE_UPDATE":
            payload = msg.get("o", {})
        else:
            return

        # Only process actual trade executions (x == "TRADE")
        if payload.get("x") != "TRADE":
            return

        trade_id = str(payload.get("t", ""))
        if not trade_id or trade_id == "-1":
            return

        event = FillEvent(
            trade_id=trade_id,
            exchange_order_id=str(payload.get("i", "")),
            client_order_id=str(payload.get("c", "")),
            symbol=str(payload.get("s", "")).upper(),
            side=str(payload.get("S", "")).upper(),
            price=float(payload.get("L", 0)),  # Last executed price
            qty=float(payload.get("l", 0)),    # Last executed quantity
            commission=float(payload.get("n", 0)),
            commission_asset=str(payload.get("N", "") or "").upper(),
            timestamp_sec=float(payload.get("T", msg.get("T", 0))) / 1000.0,
        )
        await self._handle_fill_event(event)

    # ------------------------------------------------------------------
    # Fallback REST Reconciliation Loop (60s safety net)
    # ------------------------------------------------------------------

    async def _reconciliation_loop(self) -> None:
        """Periodic REST /myTrades fallback reconciliation."""
        await asyncio.sleep(15.0)
        cycle = 0
        while self._running:
            try:
                cycle += 1
                await self.reconcile()
                if cycle % 10 == 0:
                    logger.info(f"Reconciliation alive: cycle #{cycle}")
                await asyncio.sleep(self.reconcile_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Reconciliation error: {e}", exc_info=True)
                await asyncio.sleep(min(self.reconcile_interval, 10.0))

    @staticmethod
    def _parse_trade_side(trade: Dict[str, Any]) -> str:
        """
        Parse trade direction across Binance Spot (/myTrades) and Futures (/userTrades).

        - Futures `/fapi/v1/userTrades` returns `"side": "BUY"|"SELL"` and `"buyer": bool`
          (it does NOT include `"isBuyer"`!).
        - Spot `/api/v3/myTrades` returns `"isBuyer": bool`.
        """
        raw_side = str(trade.get("side") or "").strip().upper()
        if raw_side in ("BUY", "SELL"):
            return raw_side
        if "isBuyer" in trade:
            return "BUY" if bool(trade.get("isBuyer")) else "SELL"
        if "buyer" in trade:
            return "BUY" if bool(trade.get("buyer")) else "SELL"
        return "SELL"

    async def reconcile(self) -> None:
        """
        Sync local order state with exchange via GET /myTrades (or Futures /userTrades).
        Processes EVERY trade_id (no dropping 2nd partial fill of an order).
        """
        symbols = set(self._symbols) | {o.symbol.upper() for o in self._orders.values() if o.symbol}
        for symbol in symbols:
            try:
                trades = await self._client.get_my_trades(symbol, limit=50)
                for trade in trades:
                    trade_id = str(trade.get("id", ""))
                    if not trade_id:
                        continue
                    ex_order_id = str(trade.get("orderId", ""))
                    # Look up client_order_id from tracked orders (or persisted TP record)
                    cid = ""
                    for o in self._orders.values():
                        if o.exchange_order_id == ex_order_id:
                            cid = o.client_order_id or ""
                            break
                    if not cid:
                        try:
                            gf = self._ledger_get_grid_fill_by_sell_order(ex_order_id)
                            if gf and gf.get("sell_client_order_id"):
                                cid = str(gf["sell_client_order_id"])
                        except Exception:
                            pass
                    event = FillEvent(
                        trade_id=trade_id,
                        exchange_order_id=ex_order_id,
                        client_order_id=cid,
                        symbol=symbol.upper(),
                        side=self._parse_trade_side(trade),
                        price=float(trade.get("price", 0)),
                        qty=float(trade.get("qty", 0)),
                        commission=float(trade.get("commission", 0)),
                        commission_asset=str(trade.get("commissionAsset", "") or "").upper(),
                        timestamp_sec=float(trade.get("time", 0)) / 1000.0,
                    )
                    await self._handle_fill_event(event)
            except Exception as e:
                logger.error(f"Reconciliation failed for {symbol}: {e}", exc_info=True)

    # ------------------------------------------------------------------
    # Unified Idempotent Fill Handler (WS + REST converge here)
    # ------------------------------------------------------------------

    async def _handle_fill_event(self, event: FillEvent) -> None:
        """
        Idempotently process a single trade execution event from either
        User Data Stream WS or REST /myTrades.
        """
        async with self._fill_lock:
            symbol = event.symbol.upper()
            sym_info = self._get_symbol_info(symbol)

            # 1. Update tracked order & ledger
            order_id = None
            matched_order: Optional[TrackedOrder] = None
            for oid, order in self._orders.items():
                if order.exchange_order_id == event.exchange_order_id:
                    order_id = oid
                    matched_order = order
                    order.status = "FILLED"
                    order.filled_quantity = event.qty
                    break

            if order_id is None:
                try:
                    oid = self._ledger_get_order_id_by_exchange(event.exchange_order_id)
                    if oid:
                        order_id = oid
                except Exception:
                    pass

            if order_id:
                self._ledger_update_fill(
                    order_id=order_id,
                    exchange_order_id=event.exchange_order_id,
                    filled_quantity=event.qty,
                    fill_price=event.price,
                )

            # 2. Detect exchange-side trend stop-loss execution
            cid = event.client_order_id or (matched_order.client_order_id if matched_order else "")
            if cid.startswith("trend_sl_") and event.trade_id not in self._counted_fills:
                self._counted_fills.add(event.trade_id)
                active_tid = self._active_trend_trade_id.pop(symbol, None)
                if active_tid:
                    try:
                        self._ledger_record_trade_close(trade_id=active_tid, exit_price=event.price)
                    except Exception:
                        pass
                if self._on_trend_stop_filled:
                    self._on_trend_stop_filled(symbol, event.price, event.qty)
                logger.warning(f"Trend STOP-LOSS filled on exchange: {symbol} {event.qty} @ {event.price}")
                return

            # Skip grid automation for trend entry/exit fills
            if cid.startswith("trend_"):
                return

            # 3. Check after_grid gate
            after_grid = (
                self.grid_deployed_at > 0
                and event.timestamp_sec > self.grid_deployed_at - 10.0
            )
            if not after_grid:
                return

            # 4. Consecutive buy circuit breaker (deduped by trade_id)
            if event.trade_id not in self._counted_fills:
                self._counted_fills.add(event.trade_id)
                if event.side == "BUY":
                    self._consecutive_buys += 1
                    if self._consecutive_buys >= self._consecutive_buy_limit:
                        logger.warning(
                            f"⚠ {self._consecutive_buys} consecutive buys since grid deploy — "
                            f"pausing new grid buys."
                        )
                else:
                    self._consecutive_buys = 0

            # 5. BUY fill → auto place TP SELL (strictly capped to this fill's net base qty!)
            if event.side == "BUY" and event.qty > 0:
                await self._place_tp_for_buy_fill(event, sym_info)

            # 6. SELL (TP) fill → mark filled + auto replenish BUY
            elif event.side == "SELL" and event.qty > 0:
                await self._handle_sell_fill(event, sym_info)

    async def _get_available_long_qty(self, symbol: str, sym_info: SymbolInfo) -> float:
        """Return available long base qty: Futures open long positionAmt, or Spot free base balance."""
        is_futures = getattr(self._client, "market", "spot") == "futures"
        if is_futures:
            try:
                positions = await self._client.get_futures_positions(symbol)
                long_amt = sum(
                    float(p.get("positionAmt", 0.0))
                    for p in positions
                    if str(p.get("symbol", "")).upper() == symbol.upper()
                    and float(p.get("positionAmt", 0.0)) > 0
                )
                return max(0.0, long_amt)
            except Exception:
                return float("inf")
        bal = await self._client.get_asset_balance(sym_info.base_asset)
        return float(bal.free)

    async def _place_tp_for_buy_fill(self, event: FillEvent, sym_info: SymbolInfo) -> None:
        tp_tag = f"{GRID_TP_PREFIX}{event.trade_id}"
        if tp_tag in self._tp_attempted:
            return
        is_futures = getattr(self._client, "market", "spot") == "futures"

        # Net base asset received after commission (if fee was charged in base asset)
        base_fee = event.commission if event.commission_asset == sym_info.base_asset else 0.0
        net_base_qty = max(0.0, event.qty - base_fee)

        buy_time_str = (
            datetime.fromtimestamp(event.timestamp_sec, tz=timezone.utc).isoformat()
            if event.timestamp_sec
            else datetime.now(timezone.utc).isoformat()
        )
        self._ledger_grid_buy_fill(
            trade_id=event.trade_id,
            symbol=event.symbol,
            exchange_order_id=event.exchange_order_id,
            buy_price=event.price,
            buy_qty=net_base_qty,
            buy_time=buy_time_str,
            profit_pct=self.grid_profit_pct,
        )

        tp_price = sym_info.round_price(event.price * self._grid_profit_mult)
        if tp_price <= event.price:
            logger.error(
                f"TP price {tp_price} <= fill {event.price} — tick rounding wiped margin. Skipping."
            )
            self._tp_attempted.add(tp_tag)
            return

        # CRITICAL: Cap sell_qty at min(available_long, net_base_qty), NEVER dump entire account!
        try:
            available = await self._get_available_long_qty(event.symbol, sym_info)
            if available <= 0:
                logger.debug(f"No available {sym_info.base_asset} long/free to place TP for {event.trade_id}")
                return
            sell_qty = sym_info.floor_qty(min(available, net_base_qty))
        except Exception:
            sell_qty = sym_info.floor_qty(net_base_qty)

        if not sym_info.is_valid_order(tp_price, sell_qty):
            logger.debug(
                f"TP skipped (below min_qty/min_notional): qty={sell_qty} @ {tp_price}"
            )
            self._tp_attempted.add(tp_tag)
            return

        # Ride price higher if market already jumped above TP
        try:
            current_px = await self._client.get_ticker_price(event.symbol)
            if current_px > tp_price:
                adjusted = sym_info.round_price(current_px * 1.001)
                if adjusted > tp_price:
                    tp_price = adjusted
        except Exception:
            pass

        try:
            tp_req = OrderRequest(
                symbol=event.symbol,
                side="SELL",
                order_type="LIMIT",
                price=tp_price,
                quantity=sell_qty,
                time_in_force="GTC",
                client_order_id=tp_tag,
                reduce_only=is_futures,
            )
            tp_resp = await self._client.place_order(tp_req)
            self._tp_attempted.add(tp_tag)
            self._ledger_grid_sell_placed(event.trade_id, tp_resp.exchange_order_id, tp_tag)
            self._orders[tp_tag] = TrackedOrder(
                order_id=tp_tag,
                exchange_order_id=tp_resp.exchange_order_id,
                client_order_id=tp_tag,
                symbol=event.symbol,
                side="SELL",
                order_type="LIMIT",
                price=tp_price,
                quantity=sell_qty,
                status="OPEN",
            )
            logger.info(f"  TP placed: SELL {sell_qty} {event.symbol} @ ${tp_price:.5f}")
        except Exception as e:
            logger.error(f"  Failed to place TP for {event.trade_id}: {e}")

    async def _handle_sell_fill(self, event: FillEvent, sym_info: SymbolInfo) -> None:
        # Mark matching grid_fills row as filled
        gf = None
        try:
            gf = self._ledger_get_grid_fill_by_sell_order(event.exchange_order_id)
            if gf:
                sell_time_str = (
                    datetime.fromtimestamp(event.timestamp_sec, tz=timezone.utc).isoformat()
                    if event.timestamp_sec
                    else datetime.now(timezone.utc).isoformat()
                )
                self._ledger_grid_sell_filled(gf["trade_id"], event.price, sell_time_str)
        except Exception:
            pass

        rebuy_tag = f"rebuy_{event.trade_id}"
        if rebuy_tag in self._rebuy_attempted:
            return

        # Only auto-cycle verified Grid TP sells (never Trend SHORT entries or manual sells)
        is_grid_tp = (gf is not None) or event.client_order_id.startswith(GRID_TP_PREFIX)
        if not is_grid_tp:
            return

        # Never replenish grid buys when grid is paused/stopped (e.g. macro bear or SHORT mutex)
        if not self.grid_active:
            logger.debug(f"  Rebuy skipped for {event.symbol}: grid is inactive")
            self._rebuy_attempted.add(rebuy_tag)
            return

        entry_price = sym_info.round_price(event.price / self._grid_profit_mult)
        if entry_price <= 0:
            return

        # Deduplicate against nearby open BUY orders (prevents 1-tick stacking after grid rebalance)
        min_gap = max(float(sym_info.tick_size) * 2.0, entry_price * 0.003)
        try:
            open_orders = await self._client.get_open_orders(event.symbol)
            for o in open_orders:
                if str(o.get("side", "")).upper() != "BUY":
                    continue
                open_px = float(o.get("price") or 0.0)
                if open_px > 0 and abs(open_px - entry_price) <= min_gap:
                    logger.info(
                        f"  ↻ Grid cycle rebuy skipped @ ${entry_price:.5f}: "
                        f"nearby BUY already open @ ${open_px:.5f}"
                    )
                    self._rebuy_attempted.add(rebuy_tag)
                    return
        except Exception as e:
            logger.debug(f"  Rebuy proximity check skipped: {e}")

        try:
            quote_bal = await self._client.get_asset_balance(sym_info.quote_asset)
            max_qty = min(event.qty, (quote_bal.free / entry_price) * 0.98)
            rebuy_qty = sym_info.floor_qty(max_qty)
        except Exception:
            rebuy_qty = sym_info.floor_qty(event.qty)

        if not sym_info.is_valid_order(entry_price, rebuy_qty):
            return

        try:
            rebuy_req = OrderRequest(
                symbol=event.symbol,
                side="BUY",
                order_type="LIMIT",
                price=entry_price,
                quantity=rebuy_qty,
                time_in_force="GTC",
                client_order_id=rebuy_tag,
            )
            rebuy_resp = await self._client.place_order(rebuy_req)
            self._rebuy_attempted.add(rebuy_tag)
            self._orders[rebuy_tag] = TrackedOrder(
                order_id=rebuy_tag,
                exchange_order_id=rebuy_resp.exchange_order_id,
                client_order_id=rebuy_tag,
                symbol=event.symbol,
                side="BUY",
                order_type="LIMIT",
                price=entry_price,
                quantity=rebuy_qty,
                status="OPEN",
            )
            logger.info(f"  ↻ Grid cycle: SELL filled → BUY {rebuy_qty} @ ${entry_price:.5f}")
        except Exception as e:
            logger.debug(f"  Rebuy skipped: {e}")
