"""
Regression tests for Futures execution, WebSocket URL migration, startup position
recovery, RiskGuard drawdown clamping, and monitor token log redaction.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.market_data import MarketDataEngine
from execution.exchange_client import (
    BalanceInfo,
    ExchangeClient,
    OrderRequest,
    OrderResponse,
    SymbolInfo,
)
from execution.order_manager import OrderManager
from monitor.web_server import TokenRedactFilter, redact_tokens
from risk.risk_guard import RiskGuard
from strategy.signal import SignalAction, SignalBuilder
from strategy.trend_strategy import TrendDirection, TrendState


# ---------------------------------------------------------------------------
# 1. ExchangeClient: _signed_request immutability + Futures WS /private URLs
# ---------------------------------------------------------------------------

class _FakeHTTPResponse:
    def __init__(self, status: int, payload: Any):
        self.status = status
        self._payload = payload

    async def json(self):
        return self._payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeSession:
    def __init__(self, responses: Dict[str, Any]):
        self.responses = responses
        self.requested_urls: List[str] = []

    def get(self, url: str, headers=None, timeout=None):
        self.requested_urls.append(url)
        path = urlparse(url).path
        return _FakeHTTPResponse(200, self.responses.get(path, []))


class TestExchangeClientFixes:
    @pytest.mark.asyncio
    async def test_signed_request_does_not_mutate_params_and_open_orders_signs_cleanly(self):
        """
        Regression for Binance API error [400]: Signature for this request is not valid.
        get_open_orders() calls /openOrders then /openAlgoOrders; both must carry valid
        fresh HMAC signatures computed WITHOUT any leaked signature parameter.
        """
        client = ExchangeClient(
            api_key="test_key",
            api_secret="test_secret",
            testnet=True,
            market="futures",
        )
        fake_sess = _FakeSession({
            "/fapi/v1/openOrders": [
                {"symbol": "DOGEUSDT", "orderId": 101, "side": "BUY", "price": "0.055", "origQty": "100"}
            ],
            "/fapi/v1/openAlgoOrders": [
                {
                    "algoId": 202,
                    "clientAlgoId": "trend_sl_xyz",
                    "symbol": "DOGEUSDT",
                    "side": "BUY",
                    "orderType": "STOP_MARKET",
                    "triggerPrice": "0.059",
                    "quantity": "1000",
                    "reduceOnly": True,
                }
            ],
        })
        client._session = fake_sess

        # Caller dict must remain unmutated across calls
        shared_params = {"symbol": "DOGEUSDT"}
        await client._signed_request("GET", "/fapi/v1/openOrders", shared_params)
        assert shared_params == {"symbol": "DOGEUSDT"}

        # get_open_orders must merge both endpoints with valid signatures
        fake_sess.requested_urls.clear()
        orders = await client.get_open_orders("DOGEUSDT")
        assert len(orders) == 2
        assert any(o.get("_is_algo") for o in orders)
        assert len(fake_sess.requested_urls) == 2

        for url in fake_sess.requested_urls:
            qs = parse_qs(urlparse(url).query)
            assert len(qs["signature"]) == 1
            sig_sent = qs["signature"][0]
            unsigned = {k: v[0] for k, v in qs.items() if k != "signature"}
            assert client._sign(unsigned) == sig_sent

    def test_futures_private_user_stream_urls(self):
        live = ExchangeClient(api_key="k", api_secret="s", testnet=False, market="futures")
        test = ExchangeClient(api_key="k", api_secret="s", testnet=True, market="futures")
        assert live.get_user_stream_ws_url("lk123") == "wss://fstream.binance.com/private/ws/lk123"
        assert test.get_user_stream_ws_url("lk123") == "wss://fstream.binancefuture.com/private/ws/lk123"


# ---------------------------------------------------------------------------
# 2. MarketDataEngine: Binance 2026 /market WS URL builder
# ---------------------------------------------------------------------------

class TestMarketDataWSUrl:
    def test_futures_market_ws_url_preserves_market_path(self):
        eng = MarketDataEngine(
            symbols=["DOGEUSDT"],
            intervals=["5m", "1h"],
            ws_base_url="wss://fstream.binance.com/market/ws",
        )
        url, streams = eng._build_combined_stream_url()
        assert streams == ["dogeusdt@kline_5m", "dogeusdt@kline_1h"]
        assert url == "wss://fstream.binance.com/market/stream?streams=dogeusdt@kline_5m/dogeusdt@kline_1h"

    def test_spot_ws_url_strips_exact_ws_suffix(self):
        eng = MarketDataEngine(
            symbols=["BTCUSDT"],
            intervals=["5m"],
            ws_base_url="wss://stream.binance.com:9443/ws",
        )
        url, _ = eng._build_combined_stream_url()
        assert url == "wss://stream.binance.com:9443/stream?streams=btcusdt@kline_5m"


# ---------------------------------------------------------------------------
# 3. OrderManager: Futures userTrades side parsing, rebuy guards & delayed ACK
# ---------------------------------------------------------------------------

class _FuturesMockClient:
    def __init__(self, long_pos: float = 0.0):
        self.market = "futures"
        self.api_key = ""
        self._long_pos = long_pos
        self._ticker_price = 0.0565
        self._my_trades: List[Dict[str, Any]] = []
        self._open_orders: List[Dict[str, Any]] = []
        self._order_statuses: Dict[str, Dict[str, Any]] = {}
        self.placed_orders: List[OrderRequest] = []
        self._next_oid = 5000
        self._market_ack_zero_once = False

    def get_symbol_info(self, symbol: str) -> SymbolInfo:
        return SymbolInfo.default_for(symbol)

    async def get_futures_positions(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        return [{"symbol": "DOGEUSDT", "positionAmt": str(self._long_pos)}]

    async def get_asset_balance(self, asset: str) -> BalanceInfo:
        return BalanceInfo(asset=asset.upper(), free=5000.0, locked=0.0)

    async def get_ticker_price(self, symbol: str) -> float:
        return self._ticker_price

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        return list(self._open_orders)

    async def get_my_trades(self, symbol: str, limit: int = 50, **kw) -> List[Dict[str, Any]]:
        return list(self._my_trades)

    async def get_order_status(self, symbol: str, order_id: Optional[str] = None, **kw) -> Dict[str, Any]:
        return self._order_statuses.get(str(order_id), {"status": "FILLED", "executedQty": "0"})

    async def place_order(self, request: OrderRequest) -> OrderResponse:
        self._next_oid += 1
        oid = str(self._next_oid)
        self.placed_orders.append(request)
        exec_qty = request.quantity if request.order_type == "MARKET" else 0.0
        status = "FILLED" if request.order_type == "MARKET" else "NEW"
        if request.order_type == "MARKET" and self._market_ack_zero_once:
            self._market_ack_zero_once = False
            exec_qty = 0.0
            status = "NEW"
            self._order_statuses[oid] = {
                "status": "FILLED",
                "executedQty": str(request.quantity),
                "cumQuote": str(request.quantity * self._ticker_price),
            }
        return OrderResponse(
            order_id=request.client_order_id or oid,
            exchange_order_id=oid,
            client_order_id=request.client_order_id or oid,
            symbol=request.symbol,
            side=request.side,
            order_type=request.order_type,
            status=status,
            price=request.price or self._ticker_price,
            quantity=request.quantity,
            executed_qty=exec_qty,
            cummulative_quote_qty=exec_qty * self._ticker_price,
            created_at="",
            updated_at="",
        )

    async def cancel_order(self, symbol: str, **kw):
        return {"status": "CANCELED"}


def _build_futures_mgr(client: _FuturesMockClient):
    grid_fills: Dict[str, Dict[str, Any]] = {}

    def buy_fill(trade_id, symbol, exchange_order_id, buy_price, buy_qty, buy_time, profit_pct):
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

    def sell_placed(trade_id, soid, scid):
        if trade_id in grid_fills:
            grid_fills[trade_id]["sell_placed"] = True
            grid_fills[trade_id]["sell_order_id"] = soid
            grid_fills[trade_id]["sell_client_order_id"] = scid

    def sell_filled(trade_id, px, ts):
        if trade_id in grid_fills:
            grid_fills[trade_id]["sell_filled"] = True

    mgr = OrderManager(
        exchange_client=client,
        ledger_record_order=lambda **kw: f"ord_{len(client.placed_orders)}",
        grid_profit_pct=1.8,
        enable_user_stream=False,
        symbols=["DOGEUSDT"],
        ledger_grid_buy_fill=buy_fill,
        ledger_grid_sell_placed=sell_placed,
        ledger_grid_sell_filled=sell_filled,
        ledger_get_pending_tp_fills=lambda sym=None: [
            r for r in grid_fills.values() if not r["sell_filled"]
        ],
        ledger_get_grid_fill_by_sell_order=lambda soid: next(
            (r for r in grid_fills.values() if r["sell_order_id"] == soid), None
        ),
    )
    mgr.grid_active = True
    mgr.grid_deployed_at = time.time() - 60.0
    return mgr, grid_fills


class TestOrderManagerFuturesFixes:
    @pytest.mark.asyncio
    async def test_futures_user_trades_buy_is_not_misclassified_as_sell(self):
        """
        Regression for order_manager.reconcile() misclassifying Futures BUY fills
        as SELL because /fapi/v1/userTrades uses 'side'/'buyer', NOT 'isBuyer'.
        """
        client = _FuturesMockClient(long_pos=1381.0)
        now_ms = int(time.time() * 1000)
        client._my_trades = [{
            "id": 183535352,
            "orderId": 2399799196,
            "symbol": "DOGEUSDT",
            "side": "BUY",
            "buyer": True,  # Futures payload — NO 'isBuyer' key!
            "price": "0.05644",
            "qty": "1381",
            "commission": "0.0156",
            "commissionAsset": "USDT",
            "time": now_ms,
        }]
        mgr, _ = _build_futures_mgr(client)
        await mgr.reconcile()

        # Must place a reduceOnly TP SELL, NOT a rebuy BUY!
        assert len(client.placed_orders) == 1
        tp = client.placed_orders[0]
        assert tp.side == "SELL"
        assert tp.reduce_only is True
        assert tp.client_order_id == "tp_183535352"
        assert mgr._consecutive_buys == 1

    @pytest.mark.asyncio
    async def test_non_grid_sell_and_nearby_open_buy_do_not_spawn_duplicate_rebuy(self):
        """
        1) An untagged Trend SHORT SELL must NOT trigger a grid rebuy.
        2) A Grid TP SELL whose rebuy price is within 0.3% of an existing open BUY
           must skip rebuy (preventing 1-tick duplicate stacking).
        """
        client = _FuturesMockClient(long_pos=0.0)
        now_ms = int(time.time() * 1000)
        mgr, grid_fills = _build_futures_mgr(client)

        # Case 1: Trend SHORT SELL (not in grid_fills, no tp_ cid) -> NO rebuy
        client._my_trades = [{
            "id": 9001, "orderId": 8001, "side": "SELL", "buyer": False,
            "price": "0.05699", "qty": "61321", "commission": "1.39",
            "commissionAsset": "USDT", "time": now_ms,
        }]
        await mgr.reconcile()
        assert client.placed_orders == []

        # Case 2: Valid Grid TP SELL, but an open BUY already exists 1 tick away (0.05545 vs 0.05544)
        grid_fills["buy_1"] = {
            "trade_id": "buy_1", "symbol": "DOGEUSDT", "exchange_order_id": "7001",
            "buy_price": 0.05544, "buy_qty": 1381.0, "buy_time": "2026-11-28T06:00:00+00:00",
            "sell_price": 0.05644, "sell_placed": True, "sell_filled": False,
            "sell_order_id": "8002", "sell_client_order_id": "tp_buy_1",
        }
        client._open_orders = [
            {"orderId": "2399799199", "symbol": "DOGEUSDT", "side": "BUY", "price": "0.05545", "origQty": "1406"}
        ]
        client._my_trades = [{
            "id": 9002, "orderId": 8002, "side": "SELL", "buyer": False,
            "price": "0.05644", "qty": "1381", "commission": "0.015",
            "commissionAsset": "USDT", "time": now_ms,
        }]
        await mgr.reconcile()
        assert grid_fills["buy_1"]["sell_filled"] is True
        assert client.placed_orders == []

    @pytest.mark.asyncio
    async def test_reconcile_futures_stale_grid_fills_clears_ghosts_when_flat_or_short(self):
        client = _FuturesMockClient(long_pos=0.0)
        mgr, grid_fills = _build_futures_mgr(client)
        for i in range(3):
            tid = f"ghost_{i}"
            grid_fills[tid] = {
                "trade_id": tid, "symbol": "DOGEUSDT", "exchange_order_id": f"b_{i}",
                "buy_price": 0.057, "buy_qty": 2000.0, "buy_time": "2026-11-27T14:00:00+00:00",
                "sell_price": 0.058, "sell_placed": False, "sell_filled": False,
                "sell_order_id": None, "sell_client_order_id": None,
            }

        # When open_long_qty > 0, nothing is cleared
        assert await mgr.reconcile_futures_stale_grid_fills("DOGEUSDT", open_long_qty=2000.0) == 0
        # When open_long_qty == 0, all 3 ghost fills are settled
        assert await mgr.reconcile_futures_stale_grid_fills("DOGEUSDT", open_long_qty=0.0, mark_price=0.0565) == 3
        assert all(g["sell_filled"] for g in grid_fills.values())

    @pytest.mark.asyncio
    async def test_dispatch_trend_places_stop_loss_even_when_market_ack_has_zero_exec_qty(self):
        client = _FuturesMockClient(long_pos=0.0)
        client._market_ack_zero_once = True
        mgr, _ = _build_futures_mgr(client)

        sig = (
            SignalBuilder(SignalAction.START_TREND, "DOGEUSDT", 0.9)
            .with_metadata("direction", "SHORT")
            .with_metadata("entry_price", 0.0565)
            .with_metadata("position_size", 5000.0)
            .with_metadata("stop_loss", 0.0585)
            .build()
        )
        tracked = await mgr.dispatch_signal(sig)
        assert len(tracked) == 1
        assert len(client.placed_orders) == 2
        assert client.placed_orders[1].order_type == "STOP_MARKET"
        assert client.placed_orders[1].side == "BUY"
        assert client.placed_orders[1].reduce_only is True


# ---------------------------------------------------------------------------
# 4. RiskGuard drawdown clamping & Monitor token redaction
# ---------------------------------------------------------------------------

class TestRiskAndSecurityFixes:
    def test_risk_summary_drawdown_is_never_negative(self):
        eq = [5000.0]
        rg = RiskGuard(
            config={"max_drawdown_pct": 15.0, "max_daily_loss_pct": 5.0},
            equity_provider=lambda: eq[0],
            positions_provider=lambda: [],
            trade_history_provider=lambda _s: [],
        )
        rg.sync_baseline(5000.0)
        eq[0] = 5025.0  # Equity rose above peak before next peak update
        summary = rg.get_risk_summary()
        assert summary["drawdown_pct"] == 0.0

    def test_token_redaction_masks_query_secrets(self):
        raw = 'GET /api/status?token=supersecret123&foo=bar HTTP/1.1 "http://1.2.3.4:8080/?token=supersecret123"'
        clean = redact_tokens(raw)
        assert "supersecret123" not in clean
        assert "token=***" in clean
        assert "&foo=bar" in clean

        flt = TokenRedactFilter()
        rec = logging.LogRecord(
            name="aiohttp.access", level=logging.INFO, pathname="", lineno=0,
            msg='127.0.0.1 "%s" 200', args=("GET /api/orders?token=abc999 HTTP/1.1",),
            exc_info=None,
        )
        assert flt.filter(rec) is True
        assert "abc999" not in rec.getMessage()
        assert "token=***" in rec.getMessage()


# ---------------------------------------------------------------------------
# 5. TradingSystem Futures startup position recovery & emergency_stop signals
# ---------------------------------------------------------------------------

class TestTradingSystemFuturesIntegration:
    @pytest.mark.asyncio
    async def test_startup_position_sync_recovers_open_short_and_clears_ghost_grid_fills(self):
        """
        Verify TradingSystem._sync_futures_positions_on_startup() recovers an open
        -41,775 DOGE SHORT into _trend_states (freezing Grid LONG and preventing
        duplicate SHORT entries) and clears stale ghost grid_fills.
        """
        from config.schema import AppConfig
        from main import TradingSystem

        # Construct a minimal TradingSystem instance without touching PostgreSQL
        ts = object.__new__(TradingSystem)
        ts.app_config = AppConfig.model_validate({
            "exchange": {"market": "futures", "testnet": True},
            "trading": {"symbols": ["DOGEUSDT"], "leverage": 1.5},
            "strategy": {"trend_following": {"allow_short": True}},
        })
        ts._trend_states = {}
        ts._cached_futures_positions = []
        ts._futures_positions_synced = False

        client = _FuturesMockClient(long_pos=0.0)

        async def fake_positions(symbol=None):
            return [{
                "symbol": "DOGEUSDT",
                "positionAmt": "-41775",
                "entryPrice": "0.05699",
                "markPrice": "0.05648",
                "unRealizedProfit": "21.30525",
            }]

        client.get_futures_positions = fake_positions
        mgr, grid_fills = _build_futures_mgr(client)
        grid_fills["old_1"] = {
            "trade_id": "old_1", "symbol": "DOGEUSDT", "exchange_order_id": "b1",
            "buy_price": 0.0573, "buy_qty": 2500.0, "buy_time": "2026-11-27T14:31:00+00:00",
            "sell_price": 0.0583, "sell_placed": False, "sell_filled": False,
            "sell_order_id": None, "sell_client_order_id": None,
        }

        ts.exchange_client = client
        ts.order_manager = mgr
        ts._ledger_get_pending_tp_fills = lambda sym=None: [
            r for r in grid_fills.values() if not r["sell_filled"]
        ]

        await ts._sync_futures_positions_on_startup()

        recovered = ts._trend_states.get("DOGEUSDT")
        assert recovered is not None
        assert recovered.is_active is True
        assert recovered.direction == TrendDirection.SHORT
        assert recovered.position_size == 41775.0
        assert recovered.entry_price == 0.05699
        assert recovered.stop_loss_price > 0.05699
        assert grid_fills["old_1"]["sell_filled"] is True
        assert len(ts._get_positions()) == 1
        assert ts._get_positions()[0]["quantity"] == -41775.0

    @pytest.mark.asyncio
    async def test_emergency_stop_builds_valid_strategy_signals(self):
        """
        Regression for emergency_stop failing with TypeError on invalid
        StrategySignal(confidence=..., source=...) kwargs.
        """
        from config.schema import AppConfig
        from core.state_machine import TradingStateMachine
        from main import TradingSystem

        ts = object.__new__(TradingSystem)
        ts.app_config = AppConfig.model_validate({
            "exchange": {"market": "futures", "testnet": True},
            "trading": {"symbols": ["DOGEUSDT"], "leverage": 1.5},
        })
        ts._manual_paused = False
        ts._cached_live_equity = 5000.0
        ts._grid_configs = {}
        ts._trend_states = {
            "DOGEUSDT": TrendState(
                symbol="DOGEUSDT",
                direction=TrendDirection.SHORT,
                entry_price=0.057,
                position_size=1000.0,
                stop_loss_price=0.059,
                trailing_stop_price=0.059,
            )
        }
        ts.state_machine = TradingStateMachine()

        dispatched = []

        class _FakeOM:
            async def dispatch_signal(self, sig):
                dispatched.append(sig)
                return []

        class _FakeEC:
            async def cancel_all_orders(self, sym):
                return []

        class _FakeNotifier:
            async def send(self, **kw):
                return None

        ts.order_manager = _FakeOM()
        ts.exchange_client = _FakeEC()
        ts.notifier = _FakeNotifier()

        res = await ts._handle_web_control("emergency_stop", {"confirm": "STOP"})
        assert res["ok"] is True
        assert [s.action for s in dispatched] == [SignalAction.STOP_TREND, SignalAction.STOP_GRID]
        assert all(s.score == 1.0 and s.strategy_id == "WebControl" for s in dispatched)
        assert ts._trend_states["DOGEUSDT"].is_active is False
