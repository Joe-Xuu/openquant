"""
Tests for free WeChatNotifier (WeCom / PushPlus / ServerChan) and MonitorWebServer.
"""

from __future__ import annotations

import os
import sys

import aiohttp
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.notifier import WeChatNotifier
from monitor.web_server import MonitorWebServer


class _FakeResp:
    def __init__(self, status: int, body: dict):
        self.status = status
        self._body = body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def json(self, content_type=None):
        return self._body


class _FakeSession:
    def __init__(self):
        self.calls = []

    def post(self, url, json=None, data=None):
        self.calls.append((url, json, data))
        if "qyapi.weixin.qq.com" in url:
            return _FakeResp(200, {"errcode": 0, "errmsg": "ok"})
        if "pushplus.plus" in url:
            return _FakeResp(200, {"code": 200, "msg": "ok"})
        return _FakeResp(200, {"code": 0, "message": "ok"})

    async def close(self):
        pass


class TestWeChatNotifier:
    @pytest.mark.asyncio
    async def test_multi_channel_dispatch_and_dedup(self):
        notifier = WeChatNotifier(
            wecom_webhook_url="https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=abc",
            pushplus_token="pp_tok_123",
            serverchan_key="SCT_123",
            min_repeat_interval_sec=60.0,
        )
        fake = _FakeSession()
        notifier._session = fake
        assert notifier.enabled is True
        assert notifier.active_channels == ["WeCom", "PushPlus", "ServerChan"]

        ok = await notifier.send("测试开仓", "DOGEUSDT LONG @ 0.10", level="SUCCESS", dedup_key="k1")
        assert ok is True
        assert len(fake.calls) == 3

        # Duplicate within min_repeat_interval_sec is suppressed
        ok2 = await notifier.send("测试开仓", "duplicate", level="SUCCESS", dedup_key="k1")
        assert ok2 is False
        assert len(fake.calls) == 3

    @pytest.mark.asyncio
    async def test_disabled_notifier_is_silent_noop(self):
        notifier = WeChatNotifier(wecom_webhook_url="", pushplus_token="", serverchan_key="")
        assert notifier.enabled is False
        assert await notifier.send("noop", "body") is False


class TestMonitorWebServer:
    @pytest.mark.asyncio
    async def test_endpoints_and_token_auth(self):
        sample = {
            "running": True,
            "state": "GRID_ACTIVE",
            "market": "futures",
            "testnet": True,
            "leverage": 1.0,
            "equity": 105.5,
            "initial_equity": 100.0,
            "pnl_usd": 5.5,
            "pnl_pct": 5.5,
            "uptime_sec": 120,
            "tick_count": 20,
            "signal_count": 3,
            "wechat_enabled": True,
            "server_time": "2026-11-27T00:00:00+00:00",
            "risk": {
                "current_drawdown_pct": 0.5,
                "max_drawdown_pct": 15.0,
                "daily_loss_pct": 0.0,
                "max_daily_loss_pct": 5.0,
                "consecutive_losses": 0,
                "circuit_breaker_triggered": False,
            },
            "symbols": [],
            "recent_trades": [],
            "equity_history": [],
        }
        srv = MonitorWebServer(status_provider=lambda: sample, host="127.0.0.1", port=0, token="secret123")
        await srv.start()
        try:
            port = srv._site._server.sockets[0].getsockname()[1]
            base = f"http://127.0.0.1:{port}"
            async with aiohttp.ClientSession() as sess:
                # /health is always open for Docker healthcheck
                async with sess.get(f"{base}/health") as r:
                    assert r.status == 200
                    assert (await r.json())["status"] == "ok"

                # Unauthorized without token
                async with sess.get(f"{base}/api/status") as r:
                    assert r.status == 401
                async with sess.get(f"{base}/") as r:
                    assert r.status == 401

                # Authorized with query token
                async with sess.get(f"{base}/api/status?token=secret123") as r:
                    assert r.status == 200
                    data = await r.json()
                    assert data["equity"] == 105.5
                    assert data["market"] == "futures"

                # Authorized HTML dashboard with header token
                async with sess.get(f"{base}/", headers={"X-Monitor-Token": "secret123"}) as r:
                    assert r.status == 200
                    html = await r.text()
                    assert "OpenQuant 实盘监控与控制台" in html
        finally:
            await srv.stop()

    @pytest.mark.asyncio
    async def test_readonly_mode_blocks_writes_without_token(self):
        called = []

        async def ctrl(action, payload):
            called.append(action)
            return {"ok": True}

        async def params(body):
            called.append("params")
            return {"ok": True}

        srv = MonitorWebServer(
            status_provider=lambda: {"running": True},
            control_handler=ctrl,
            params_handler=params,
            host="127.0.0.1",
            port=0,
            token="",
        )
        await srv.start()
        try:
            port = srv._site._server.sockets[0].getsockname()[1]
            base = f"http://127.0.0.1:{port}"
            async with aiohttp.ClientSession() as sess:
                async with sess.get(f"{base}/api/status") as r:
                    assert r.status == 200
                    data = await r.json()
                    assert data["controls_enabled"] is False

                async with sess.post(f"{base}/api/control", json={"action": "pause"}) as r:
                    assert r.status == 403
                async with sess.post(f"{base}/api/params", json={"max_drawdown_pct": 12}) as r:
                    assert r.status == 403
                assert called == []
        finally:
            await srv.stop()

    @pytest.mark.asyncio
    async def test_authenticated_control_and_params_endpoints(self):
        state = {"paused": False, "max_drawdown_pct": 15.0}

        async def ctrl(action, payload):
            if action == "pause":
                state["paused"] = True
                return {"ok": True, "paused": True}
            if action == "resume":
                state["paused"] = False
                return {"ok": True, "paused": False}
            if action == "emergency_stop":
                if payload.get("confirm") != "STOP":
                    raise ValueError("Confirmation 'STOP' required")
                state["paused"] = True
                return {"ok": True, "paused": True}
            raise ValueError(f"Unknown action: {action}")

        async def params(body):
            dd = float(body.get("max_drawdown_pct", 15.0))
            if dd < 5.0 or dd > 35.0:
                raise ValueError("max_drawdown_pct out of bounds")
            state["max_drawdown_pct"] = dd
            return {"ok": True, "params": {"max_drawdown_pct": dd}}

        srv = MonitorWebServer(
            status_provider=lambda: {"running": True, "paused": state["paused"]},
            control_handler=ctrl,
            params_handler=params,
            host="127.0.0.1",
            port=0,
            token="tok999",
        )
        await srv.start()
        try:
            port = srv._site._server.sockets[0].getsockname()[1]
            base = f"http://127.0.0.1:{port}"
            hdrs = {"X-Monitor-Token": "tok999"}
            async with aiohttp.ClientSession() as sess:
                # Wrong token -> 401
                async with sess.post(f"{base}/api/control", json={"action": "pause"}, headers={"X-Monitor-Token": "wrong"}) as r:
                    assert r.status == 401

                # Pause / resume
                async with sess.post(f"{base}/api/control", json={"action": "pause"}, headers=hdrs) as r:
                    assert r.status == 200
                    assert (await r.json())["paused"] is True
                async with sess.post(f"{base}/api/control", json={"action": "resume"}, headers=hdrs) as r:
                    assert r.status == 200
                    assert (await r.json())["paused"] is False

                # Emergency stop requires confirm == "STOP"
                async with sess.post(f"{base}/api/control", json={"action": "emergency_stop", "confirm": "NO"}, headers=hdrs) as r:
                    assert r.status == 400
                async with sess.post(f"{base}/api/control", json={"action": "emergency_stop", "confirm": "STOP"}, headers=hdrs) as r:
                    assert r.status == 200
                    assert state["paused"] is True

                # Params valid + out-of-bounds
                async with sess.post(f"{base}/api/params", json={"max_drawdown_pct": 20.0}, headers=hdrs) as r:
                    assert r.status == 200
                    assert state["max_drawdown_pct"] == 20.0
                async with sess.post(f"{base}/api/params", json={"max_drawdown_pct": 99.0}, headers=hdrs) as r:
                    assert r.status == 400
        finally:
            await srv.stop()

    @pytest.mark.asyncio
    async def test_open_orders_and_manual_cancel(self):
        orders = [
            {"symbol": "DOGEUSDT", "order_id": "101", "tag": "网格买单", "side": "BUY", "type": "LIMIT", "price": 0.055, "orig_qty": 100.0, "executed_qty": 0.0, "reduce_only": False, "time": ""},
            {"symbol": "DOGEUSDT", "order_id": "102", "tag": "网格止盈TP", "side": "SELL", "type": "LIMIT", "price": 0.060, "orig_qty": 100.0, "executed_qty": 0.0, "reduce_only": True, "time": ""},
        ]

        async def fetch_orders(_force: bool):
            return list(orders)

        async def cancel_order(body):
            if body.get("cancel_all"):
                orders.clear()
                return {"ok": True, "message": "all cancelled"}
            oid = str(body.get("order_id", ""))
            before = len(orders)
            orders[:] = [o for o in orders if o["order_id"] != oid]
            if len(orders) == before:
                raise ValueError("order not found")
            return {"ok": True, "message": f"cancelled {oid}"}

        srv = MonitorWebServer(
            status_provider=lambda: {"running": True},
            orders_provider=fetch_orders,
            cancel_order_handler=cancel_order,
            host="127.0.0.1",
            port=0,
            token="tok_ord",
        )
        await srv.start()
        try:
            port = srv._site._server.sockets[0].getsockname()[1]
            base = f"http://127.0.0.1:{port}"
            hdrs = {"X-Monitor-Token": "tok_ord"}
            async with aiohttp.ClientSession() as sess:
                # Unauthorized without token
                async with sess.get(f"{base}/api/orders") as r:
                    assert r.status == 401

                # Fetch orders
                async with sess.get(f"{base}/api/orders?force=1", headers=hdrs) as r:
                    assert r.status == 200
                    assert len((await r.json())["orders"]) == 2

                # Cancel single order
                async with sess.post(f"{base}/api/orders/cancel", json={"symbol": "DOGEUSDT", "order_id": "101"}, headers=hdrs) as r:
                    assert r.status == 200
                async with sess.get(f"{base}/api/orders", headers=hdrs) as r:
                    rem = (await r.json())["orders"]
                    assert len(rem) == 1 and rem[0]["order_id"] == "102"

                # Cancel all remaining
                async with sess.post(f"{base}/api/orders/cancel", json={"cancel_all": True, "symbol": "ALL"}, headers=hdrs) as r:
                    assert r.status == 200
                async with sess.get(f"{base}/api/orders", headers=hdrs) as r:
                    assert (await r.json())["orders"] == []
        finally:
            await srv.stop()

    @pytest.mark.asyncio
    async def test_klines_endpoint(self):
        async def fake_klines(sym: str, interval: str, limit: int):
            return {
                "symbol": sym or "DOGEUSDT",
                "interval": interval,
                "price": 0.06123,
                "klines": [
                    {"t": "2026-11-27T01:00:00+00:00", "o": 0.060, "h": 0.062, "l": 0.059, "c": 0.06123, "v": 12345.0}
                ],
            }

        srv = MonitorWebServer(
            status_provider=lambda: {"running": True},
            klines_provider=fake_klines,
            host="127.0.0.1",
            port=0,
            token="tok_k",
        )
        await srv.start()
        try:
            port = srv._site._server.sockets[0].getsockname()[1]
            base = f"http://127.0.0.1:{port}"
            async with aiohttp.ClientSession() as sess:
                async with sess.get(f"{base}/api/klines") as r:
                    assert r.status == 401
                async with sess.get(f"{base}/api/klines?interval=15m&limit=60", headers={"X-Monitor-Token": "tok_k"}) as r:
                    assert r.status == 200
                    d = await r.json()
                    assert d["symbol"] == "DOGEUSDT"
                    assert d["interval"] == "15m"
                    assert d["price"] == 0.06123
                    assert len(d["klines"]) == 1
        finally:
            await srv.stop()


class TestRiskGuardReset:
    def test_reset_risk_state_clears_circuit_breaker(self):
        from risk.risk_guard import RiskGuard

        eq = [1000.0]
        rg = RiskGuard(
            config={"max_drawdown_pct": 10.0, "max_daily_loss_pct": 5.0},
            equity_provider=lambda: eq[0],
            positions_provider=lambda: [],
            trade_history_provider=lambda _s: [],
        )
        rg.sync_baseline(1000.0)
        eq[0] = 850.0
        rg._circuit_breaker = True
        rg._circuit_breaker_reason = "drawdown"

        rg.reset_risk_state(850.0)
        summary = rg.get_risk_summary()
        assert summary["circuit_breaker_triggered"] is False
        assert summary["peak_equity"] == 850.0
        assert summary["drawdown_pct"] == 0.0

