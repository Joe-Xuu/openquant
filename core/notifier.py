"""
================================================================================
WECHAT NOTIFIER — Free Real-Time Trade & Risk Alerts
================================================================================

Supports free WeChat notification channels (configure any in .env):
  1. 企业微信群机器人 Webhook (WECHAT_WEBHOOK_URL) — 100% free, unlimited
  2. PushPlus 免费微信推送     (PUSHPLUS_TOKEN)     — free 200 msgs/day to personal WeChat
  3. Server酱 Turbo            (SERVERCHAN_KEY)     — free tier to personal WeChat

All sends are async, non-blocking, timeout-protected, and rate-limited so
network errors never affect the trading loop.
================================================================================
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Dict, Optional

import aiohttp

logger = logging.getLogger("core.notifier")


class WeChatNotifier:
    """Async multi-channel free WeChat alert dispatcher."""

    def __init__(
        self,
        wecom_webhook_url: Optional[str] = None,
        pushplus_token: Optional[str] = None,
        serverchan_key: Optional[str] = None,
        min_repeat_interval_sec: float = 30.0,
    ):
        self.wecom_webhook_url = (wecom_webhook_url if wecom_webhook_url is not None else os.getenv("WECHAT_WEBHOOK_URL", "")).strip()
        self.pushplus_token = (pushplus_token if pushplus_token is not None else os.getenv("PUSHPLUS_TOKEN", "")).strip()
        self.serverchan_key = (serverchan_key if serverchan_key is not None else os.getenv("SERVERCHAN_KEY", "")).strip()
        self.min_repeat_interval_sec = min_repeat_interval_sec

        self._session: Optional[aiohttp.ClientSession] = None
        self._last_sent_by_key: Dict[str, float] = {}

    @property
    def enabled(self) -> bool:
        return bool(self.wecom_webhook_url or self.pushplus_token or self.serverchan_key)

    @property
    def active_channels(self) -> list[str]:
        ch = []
        if self.wecom_webhook_url:
            ch.append("WeCom")
        if self.pushplus_token:
            ch.append("PushPlus")
        if self.serverchan_key:
            ch.append("ServerChan")
        return ch

    async def start(self) -> None:
        if self.enabled and self._session is None:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8))
            logger.info(f"WeChatNotifier started (channels={', '.join(self.active_channels)})")

    async def stop(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    def _should_send(self, dedup_key: Optional[str]) -> bool:
        if not dedup_key:
            return True
        now = time.monotonic()
        last = self._last_sent_by_key.get(dedup_key, 0.0)
        if now - last < self.min_repeat_interval_sec:
            return False
        self._last_sent_by_key[dedup_key] = now
        return True

    async def send(
        self,
        title: str,
        body: str,
        level: str = "INFO",
        dedup_key: Optional[str] = None,
    ) -> bool:
        """Send a notification to all configured free WeChat channels."""
        if not self.enabled or not self._should_send(dedup_key):
            return False

        if self._session is None:
            await self.start()

        prefix = {"INFO": "ℹ️", "SUCCESS": "✅", "WARN": "⚠️", "ERROR": "🚨"}.get(level.upper(), "📢")
        full_title = f"{prefix} OpenQuant | {title}"
        md_text = f"### {full_title}\n{body}"

        tasks = []
        if self.wecom_webhook_url:
            tasks.append(self._send_wecom(md_text))
        if self.pushplus_token:
            tasks.append(self._send_pushplus(full_title, body))
        if self.serverchan_key:
            tasks.append(self._send_serverchan(full_title, body))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        return any(r is True for r in results)

    def send_background(
        self,
        title: str,
        body: str,
        level: str = "INFO",
        dedup_key: Optional[str] = None,
    ) -> None:
        """Fire-and-forget wrapper safe to call from trading hot paths."""
        if not self.enabled:
            return
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.send(title, body, level=level, dedup_key=dedup_key))
        except RuntimeError:
            pass

    async def _send_wecom(self, markdown_content: str) -> bool:
        assert self._session is not None
        payload = {"msgtype": "markdown", "markdown": {"content": markdown_content}}
        try:
            async with self._session.post(self.wecom_webhook_url, json=payload) as resp:
                data = await resp.json(content_type=None)
                if resp.status == 200 and data.get("errcode", 0) == 0:
                    return True
                logger.warning(f"WeCom webhook error: status={resp.status} body={data}")
                return False
        except Exception as e:
            logger.warning(f"WeCom webhook failed: {e}")
            return False

    async def _send_pushplus(self, title: str, content: str) -> bool:
        assert self._session is not None
        payload = {
            "token": self.pushplus_token,
            "title": title,
            "content": content,
            "template": "markdown",
        }
        try:
            async with self._session.post("https://www.pushplus.plus/send", json=payload) as resp:
                data = await resp.json(content_type=None)
                if resp.status == 200 and data.get("code") == 200:
                    return True
                logger.warning(f"PushPlus error: status={resp.status} body={data}")
                return False
        except Exception as e:
            logger.warning(f"PushPlus failed: {e}")
            return False

    async def _send_serverchan(self, title: str, desp: str) -> bool:
        assert self._session is not None
        url = f"https://sctapi.ftqq.com/{self.serverchan_key}.send"
        try:
            async with self._session.post(url, data={"title": title, "desp": desp}) as resp:
                data = await resp.json(content_type=None)
                if resp.status == 200 and data.get("code") == 0:
                    return True
                logger.warning(f"ServerChan error: status={resp.status} body={data}")
                return False
        except Exception as e:
            logger.warning(f"ServerChan failed: {e}")
            return False
