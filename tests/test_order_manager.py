"""
Integration tests for OrderManager, SymbolInfo, AsyncTokenBucket, and AppConfig:
    - Partial-fill safety (2 fills for same orderId both get TP orders)
    - TP sell qty strictly capped to min(available, net_fill_qty) — never dumps account
    - Startup sync_tp_orders() -> recover_pending_tp_fills() re-places cancelled TP
    - Strict prefix isolation: trend exit/modify never cancels grid/TP orders
    - Real-time WS executionReport + REST reconcile idempotency (zero duplicate TPs)
    - Trend stop-loss fill callback triggers state reset
"""

import asyncio
import sys
import time
from typing import Any, Dict, List, Optional

import pytest

sys.path.insert(0, ".")

from config.schema import AppConfig
from execution.exchange_client import AsyncTokenBucket, BalanceInfo, OrderRequest, OrderResponse, SymbolInfo
from execution.order_manager import OrderManager
from strategy.signal import SignalAction, SignalBuilder


class MockExchangeClient:
    """Deterministic in-memory mock Binance client for OrderManager tests."""

    def __init__(self, balances: Optional[Dict[str, float]] = None, ticker_price: float = 0.08):
        self.api_key = ""
        self._balances = {k.upper(): v for k, v in (balances or {"USDT": 100.0, "DOGE": 500.0}).items()}
        self._ticker_price = ticker_price
        self._open_orders: List[Dict[str, Any]] = []
        self._order_statuses: Dict[str, Dict[str, Any]] = {}
        self._my_trades: List[Dict[str, Any]] = []
        self.placed_orders: List[OrderRequest] = []
        self.cancelled_order_ids: List[str] = []
        self._next_oid = 1000
        self._sym_info = {
            "DOGEUSDT": SymbolInfo.default_for("DOGEUSDT"),
            "BTCUSDT": SymbolInfo.default_for("BTCUSDT"),
        }

    def get_symbol_info(self, symbol: str) -> SymbolInfo:
        return self._sym_info.get(symbol.upper(), SymbolInfo.default_for(symbol))

    async def get_asset_balance(self, asset: str) -> BalanceInfo:
        free = self._balances.get(asset.upper(), 0.0)
        return BalanceInfo(asset=asset.upper(), free=free, locked=0.0)

    async def get_ticker_price(self, symbol: str) -> float:
        return self._ticker_price

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        return list(self._open_orders)

    async def get_order_status(
        self, symbol: str, order_id: Optional[str] = None, client_order_id: Optional[str] = None
    ) -> Dict[str, Any]:
        if order_id and order_id in self._order_statuses:
            return self._order_statuses[order_id]
        if client_order_id and client_order_id in self._order_statuses:
            return self._order_statuses[client_order_id]
        raise Exception("Binance API error [400]: Order does not exist (-2013)")

    async def get_my_trades(self, symbol: str, limit: int = 50, **kw) -> List[Dict[str, Any]]:
        return list(self._my_trades)

    async def place_order(self, request: OrderRequest) -> OrderResponse:
        self._next_oid += 1
        oid = str(self._next_oid)
        self.placed_orders.append(request)
        if request.side == "SELL" and request.order_type == "LIMIT":
            base = self.get_symbol_info(request.symbol).base_asset
            self._balances[base] = max(0.0, self._balances.get(base, 0.0) - request.quantity)
        self._open_orders.append({
            "orderId": oid,
            "clientOrderId": request.client_order_id or "",
            "symbol": request.symbol,
            "side": request.side,
            "price": str(request.price or 0.0),
            "origQty": str(request.quantity),
        })
        return OrderResponse(
            order_id=request.client_order_id or oid,
            exchange_order_id=oid,
            client_order_id=request.client_order_id or oid,
            symbol=request.symbol,
            side=request.side,
            order_type=request.order_type,
            status="FILLED" if request.order_type == "MARKET" else "NEW",
            price=request.price or self._ticker_price,
            quantity=request.quantity,
            executed_qty=request.quantity if request.order_type == "MARKET" else 0.0,
            cummulative_quote_qty=(request.quantity * self._ticker_price) if request.order_type == "MARKET" else 0.0,
            created_at="",
            updated_at="",
        )

    async def cancel_order(self, symbol: str, order_id: Optional[str] = None, client_order_id: Optional[str] = None):
        self.cancelled_order_ids.append(order_id or client_order_id or "")
        self._open_orders = [
            o for o in self._open_orders
            if str(o.get("orderId")) != str(order_id) and o.get("clientOrderId") != client_order_id
        ]
        return {"status": "CANCELED"}

    async def cancel_all_orders(self, symbol: str):
        self._open_orders.clear()
        return []


def _make_manager(client: MockExchangeClient, on_trend_stop=None) -> tuple[OrderManager, dict]:
    """Build an OrderManager wired to an in-memory grid_fills store."""
    grid_fills: Dict[str, Dict[str, Any]] = {}

    def buy_fill(trade_id, symbol, exchange_order_id, buy_price, buy_qty, buy_time, profit_pct):
        if trade_id in grid_fills:
            return False
        grid_fills[trade_id] = {
            "trade_id": trade_id,
            "symbol": symbol,
            "exchange_order_id": exchange_order_id,
            "buy_price": buy_price,
            "buy_qty": buy_qty,
            "buy_time": buy_time,
            "sell_price": buy_price * (1.0 + profit_pct / 100.0),
            "sell_placed": False,
            "sell_filled": False,
            "sell_order_id": None,
            "sell_client_order_id": None,
        }
        return True

    def sell_placed(trade_id, sell_oid, sell_cid):
        if trade_id in grid_fills:
            grid_fills[trade_id]["sell_placed"] = True
            grid_fills[trade_id]["sell_order_id"] = sell_oid
            grid_fills[trade_id]["sell_client_order_id"] = sell_cid

    def sell_filled(trade_id, px, ts):
        if trade_id in grid_fills:
            grid_fills[trade_id]["sell_filled"] = True

    def reset_sell(trade_id):
        if trade_id in grid_fills:
            grid_fills[trade_id]["sell_placed"] = False
            grid_fills[trade_id]["sell_order_id"] = None
            grid_fills[trade_id]["sell_client_order_id"] = None

    mgr = OrderManager(
        exchange_client=client,
        ledger_record_order=lambda **kw: f"ord_{len(client.placed_orders)}",
        grid_profit_pct=1.0,
        enable_user_stream=False,
        symbols=["DOGEUSDT"],
        ledger_grid_buy_fill=buy_fill,
        ledger_grid_sell_placed=sell_placed,
        ledger_grid_sell_filled=sell_filled,
        ledger_grid_reset_sell_placed=reset_sell,
        ledger_get_pending_tp_fills=lambda sym=None: [
            r for r in grid_fills.values() if not r["sell_filled"]
        ],
        ledger_get_tp_attempted_tags=lambda sym=None: [
            f"tp_{r['trade_id']}" for r in grid_fills.values() if r["sell_placed"]
        ],
        ledger_get_grid_fill_by_sell_order=lambda soid: next(
            (r for r in grid_fills.values() if r["sell_order_id"] == soid), None
        ),
        on_trend_stop_filled=on_trend_stop,
    )
    mgr.grid_active = True
    mgr.grid_deployed_at = time.time() - 60.0
    return mgr, grid_fills


class TestOrderManagerSafety:
    @pytest.mark.asyncio
    async def test_tp_caps_sell_qty_and_handles_partial_fills(self):
        """
        Account has 1000 DOGE free, but a BUY order partially fills in two trades:
        30 DOGE (trade 501) and 20 DOGE (trade 502) under the SAME orderId=9001.
        Both partial fills must get separate TP orders, each capped to its fill qty
        (NOT dumping all 1000 DOGE!).
        """
        client = MockExchangeClient(balances={"USDT": 100.0, "DOGE": 1000.0}, ticker_price=0.08)
        now_ms = int(time.time() * 1000)
        client._my_trades = [
            {
                "id": 501, "orderId": 9001, "price": "0.08000", "qty": "30",
                "commission": "0.03", "commissionAsset": "DOGE", "time": now_ms, "isBuyer": True,
            },
            {
                "id": 502, "orderId": 9001, "price": "0.08000", "qty": "20",
                "commission": "0.02", "commissionAsset": "DOGE", "time": now_ms, "isBuyer": True,
            },
        ]
        mgr, _ = _make_manager(client)
        await mgr.reconcile()

        tp_orders = [o for o in client.placed_orders if o.side == "SELL"]
        assert len(tp_orders) == 2
        # Floor(30 - 0.03) = 29 DOGE, Floor(20 - 0.02) = 19 DOGE — never 1000 DOGE!
        assert sorted(o.quantity for o in tp_orders) == [19.0, 29.0]

    @pytest.mark.asyncio
    async def test_sync_then_recover_replaces_cancelled_tp(self):
        """If a previously placed TP was cancelled on exchange, sync + recover must re-place it."""
        client = MockExchangeClient(balances={"USDT": 50.0, "DOGE": 100.0}, ticker_price=0.08)
        client._order_statuses["old_tp_oid"] = {"status": "CANCELED"}
        mgr, grid_fills = _make_manager(client)
        grid_fills["t_99"] = {
            "trade_id": "t_99", "symbol": "DOGEUSDT", "exchange_order_id": "buy_99",
            "buy_price": 0.08, "buy_qty": 50.0, "buy_time": "2026-01-01T00:00:00+00:00",
            "sell_price": 0.0808, "sell_placed": True, "sell_filled": False,
            "sell_order_id": "old_tp_oid", "sell_client_order_id": "tp_t_99",
        }
        await mgr._seed_tp_cache()
        assert "tp_t_99" in mgr._tp_attempted

        corrected = await mgr.sync_tp_orders()
        assert corrected == 1
        assert "tp_t_99" not in mgr._tp_attempted

        recovered = await mgr.recover_pending_tp_fills()
        assert recovered == 1
        assert len(client.placed_orders) == 1
        assert client.placed_orders[0].quantity == 50.0

    @pytest.mark.asyncio
    async def test_prefix_isolation_trend_exit_preserves_grid_and_tp(self):
        """Closing a trend position must ONLY cancel trend_sl_*/trend_* orders, never grid/TP."""
        client = MockExchangeClient(balances={"USDT": 100.0, "DOGE": 500.0}, ticker_price=0.08)
        client._open_orders = [
            {"orderId": "1", "clientOrderId": "grid_abc", "symbol": "DOGEUSDT"},
            {"orderId": "2", "clientOrderId": "tp_501", "symbol": "DOGEUSDT"},
            {"orderId": "3", "clientOrderId": "trend_sl_xyz", "symbol": "DOGEUSDT"},
        ]
        mgr, _ = _make_manager(client)

        stop_sig = (
            SignalBuilder(SignalAction.STOP_TREND, "DOGEUSDT", 1.0)
            .with_metadata("direction", "LONG")
            .with_metadata("position_size", 100.0)
            .with_metadata("exit_price", 0.08)
            .build()
        )
        await mgr.dispatch_signal(stop_sig)

        assert client.cancelled_order_ids == ["3"]
        remaining_cids = {o["clientOrderId"] for o in client._open_orders}
        assert "grid_abc" in remaining_cids
        assert "tp_501" in remaining_cids

    @pytest.mark.asyncio
    async def test_ws_execution_report_and_rest_idempotent(self):
        """WS executionReport places TP immediately; subsequent REST reconcile does NOT duplicate."""
        stopped = []
        client = MockExchangeClient(balances={"USDT": 100.0, "DOGE": 500.0}, ticker_price=0.08)
        mgr, _ = _make_manager(client, on_trend_stop=lambda s, p, q: stopped.append((s, p, q)))
        now_ms = int(time.time() * 1000)

        # 1) WS grid BUY executionReport
        ws_buy = {
            "e": "executionReport", "x": "TRADE", "s": "DOGEUSDT", "S": "BUY",
            "t": 777, "i": 8888, "c": "grid_12345",
            "L": "0.08000", "l": "50", "n": "0.05", "N": "DOGE", "T": now_ms,
        }
        await mgr._handle_user_stream_message(ws_buy)
        assert len(client.placed_orders) == 1
        assert client.placed_orders[0].client_order_id == "tp_777"
        assert client.placed_orders[0].quantity == 49.0

        # 2) REST reconcile returns the exact same trade -> must NOT place a duplicate TP
        client._my_trades = [{
            "id": 777, "orderId": 8888, "price": "0.08000", "qty": "50",
            "commission": "0.05", "commissionAsset": "DOGE", "time": now_ms, "isBuyer": True,
        }]
        await mgr.reconcile()
        assert len(client.placed_orders) == 1

        # 3) WS trend stop-loss fill triggers on_trend_stop_filled callback
        ws_sl = {
            "e": "executionReport", "x": "TRADE", "s": "DOGEUSDT", "S": "SELL",
            "t": 778, "i": 8889, "c": "trend_sl_abc",
            "L": "0.07800", "l": "100", "n": "0.01", "N": "USDT", "T": now_ms,
        }
        await mgr._handle_user_stream_message(ws_sl)
        assert stopped == [("DOGEUSDT", 0.078, 100.0)]

    @pytest.mark.asyncio
    async def test_stop_grid_liquidate_inventory_preserves_trend(self):
        """Macro-bear STOP_GRID with liquidate_inventory cancels grid+TP and sells grid lots only."""
        client = MockExchangeClient(balances={"USDT": 50.0, "DOGE": 300.0}, ticker_price=0.08)
        client._open_orders = [
            {"orderId": "1", "clientOrderId": "grid_abc", "symbol": "DOGEUSDT"},
            {"orderId": "2", "clientOrderId": "tp_501", "symbol": "DOGEUSDT"},
            {"orderId": "3", "clientOrderId": "trend_sl_xyz", "symbol": "DOGEUSDT"},
        ]
        mgr, grid_fills = _make_manager(client)
        grid_fills["t_501"] = {
            "trade_id": "t_501", "symbol": "DOGEUSDT", "exchange_order_id": "buy_501",
            "buy_price": 0.082, "buy_qty": 100.0, "buy_time": "2026-01-01T00:00:00+00:00",
            "sell_price": 0.0835, "sell_placed": True, "sell_filled": False,
            "sell_order_id": "2", "sell_client_order_id": "tp_501",
        }
        stop_sig = (
            SignalBuilder(SignalAction.STOP_GRID, "DOGEUSDT", 1.0)
            .with_metadata("grid_id", "g1")
            .with_metadata("reason", "macro_bear")
            .with_metadata("liquidate_inventory", True)
            .build()
        )
        await mgr.dispatch_signal(stop_sig)

        # Cancelled grid + TP, preserved trend_sl_xyz
        assert sorted(client.cancelled_order_ids) == ["1", "2"]
        remaining = {o["clientOrderId"] for o in client._open_orders}
        assert "trend_sl_xyz" in remaining
        assert "grid_abc" not in remaining
        assert "tp_501" not in remaining

        # Placed exactly one MARKET SELL for pending grid qty (100 DOGE, NOT all 300 DOGE)
        assert len(client.placed_orders) == 1
        sell = client.placed_orders[0]
        assert sell.side == "SELL"
        assert sell.order_type == "MARKET"
        assert sell.quantity == 100.0
        assert grid_fills["t_501"]["sell_filled"] is True


class TestSymbolInfoAndInfra:
    def test_symbol_info_precision_and_notional(self):
        info = SymbolInfo.default_for("DOGEUSDT")
        assert info.round_price(0.08123456) == 0.08123
        assert info.floor_qty(19.987) == 19.0
        assert info.is_valid_order(0.08, 20.0) is True   # $1.60 >= $1.00
        assert info.is_valid_order(0.08, 5.0) is False   # $0.40 < $1.00

    @pytest.mark.asyncio
    async def test_token_bucket_enforces_rate(self):
        bucket = AsyncTokenBucket(rate=20.0, capacity=2.0)
        t0 = time.monotonic()
        for _ in range(4):  # 2 burst + 2 refilled at 20/s -> ~0.10s
            async with bucket:
                pass
        elapsed = time.monotonic() - t0
        assert elapsed >= 0.08

    def test_app_config_enforces_spot_no_short_and_capital_sum(self):
        cfg = AppConfig.model_validate({
            "exchange": {"market": "spot"},
            "strategy": {"trend_following": {"allow_short": True}},
        })
        assert cfg.strategy.trend_following.allow_short is False

        with pytest.raises(ValueError):
            AppConfig.model_validate({
                "trading": {"grid_capital_pct": 0.80, "trend_capital_pct": 0.30}
            })

    def test_settings_json_loads_v4c_defaults(self):
        from config.schema import load_app_config
        cfg = load_app_config("config/settings.json")
        assert cfg.trading.grid_capital_pct == 0.45
        assert cfg.trading.trend_capital_pct == 0.50
        assert cfg.data.trend_interval == "1h"
        assert cfg.strategy.macro_filter.enabled is True
        assert cfg.strategy.macro_filter.interval in cfg.data.kline_intervals
        assert cfg.strategy.grid_trading.spot_mode is True
        assert cfg.strategy.trend_following.trailing_stop_atr_multiplier == 4.0

    def test_futures_leverage_cap_and_spot_enforcement(self):
        # Spot always clamps leverage to 1.0 and disables short
        spot = AppConfig.model_validate({
            "exchange": {"market": "spot"},
            "trading": {"leverage": 2.0},
            "strategy": {"trend_following": {"allow_short": True}},
        })
        assert spot.trading.leverage == 1.0
        assert spot.strategy.trend_following.allow_short is False

        # Futures allows up to 3.0x and syncs risk max_leverage
        fut = AppConfig.model_validate({
            "exchange": {"market": "futures"},
            "trading": {"leverage": 1.5, "margin_type": "ISOLATED"},
            "strategy": {"trend_following": {"allow_short": True}},
        })
        assert fut.trading.leverage == 1.5
        assert fut.risk.max_leverage >= 1.5
        assert fut.strategy.trend_following.allow_short is True

        # Leverage > 3.0 is rejected by schema hard cap
        with pytest.raises(ValueError):
            AppConfig.model_validate({"exchange": {"market": "futures"}, "trading": {"leverage": 5.0}})

    @pytest.mark.asyncio
    async def test_futures_short_dispatch_and_order_trade_update(self):
        """Futures SHORT entry uses STOP_MARKET reduceOnly SL, MARKET reduceOnly exit, and ORDER_TRADE_UPDATE WS."""
        stopped = []
        client = MockExchangeClient(balances={"USDT": 100.0}, ticker_price=0.10)
        client.market = "futures"
        mgr, _ = _make_manager(client, on_trend_stop=lambda s, p, q: stopped.append((s, p, q)))

        # 1) Dispatch SHORT entry -> SELL MARKET + BUY STOP_MARKET reduceOnly
        entry_sig = (
            SignalBuilder(SignalAction.START_TREND, "DOGEUSDT", 0.9)
            .with_metadata("direction", "SHORT")
            .with_metadata("entry_price", 0.10)
            .with_metadata("position_size", 500.0)
            .with_metadata("stop_loss", 0.105)
            .build()
        )
        await mgr.dispatch_signal(entry_sig)
        assert len(client.placed_orders) == 2
        entry_o, sl_o = client.placed_orders
        assert entry_o.side == "SELL" and entry_o.order_type == "MARKET"
        assert sl_o.side == "BUY" and sl_o.order_type == "STOP_MARKET" and sl_o.reduce_only is True

        # 2) Futures ORDER_TRADE_UPDATE WS stop-loss fill triggers callback
        now_ms = int(time.time() * 1000)
        fut_ws = {
            "e": "ORDER_TRADE_UPDATE",
            "T": now_ms,
            "o": {
                "s": "DOGEUSDT", "c": sl_o.client_order_id, "S": "BUY",
                "x": "TRADE", "i": 9999, "t": 8881,
                "L": "0.1051", "l": "500", "n": "0.02", "N": "USDT", "T": now_ms,
            },
        }
        await mgr._handle_user_stream_message(fut_ws)
        assert stopped == [("DOGEUSDT", 0.1051, 500.0)]


