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
                    assert "OpenQuant 实盘实时监控" in html
        finally:
            await srv.stop()
