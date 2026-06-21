"""
================================================================================
MARKET DATA — REST Kline Fetcher & WebSocket Stream Manager
================================================================================

Provides async interfaces for fetching historical OHLCV data (REST) and
subscribing to real-time kline streams (WebSocket) from Binance.

DESIGN:
    - REST: Fetches initial historical data for indicator warm-up.
    - WebSocket: Maintains a persistent stream of live kline updates.
    - All data is normalized to the standard OHLCV dict format.
    - Thread-safe buffer for the most recent candles (async-safe with asyncio.Queue).

DEPENDENCIES:
    - aiohttp (async HTTP + WebSocket)
    - asyncio (standard library)

USAGE:
    engine = MarketDataEngine(symbols=["BTCUSDT"], primary_interval="5m")
    await engine.start()
    # ... engine.ohlcv_buffers["BTCUSDT"] is updated in real-time
    await engine.stop()
================================================================================
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger("data.market_data")


# ---------------------------------------------------------------------------
# Data Structures
# ---------------------------------------------------------------------------

@dataclass
class OHLCVBar:
    """A single OHLCV candle."""
    open: float
    high: float
    low: float
    close: float
    volume: float
    timestamp: str  # ISO-8601
    is_closed: bool = True  # False if this is the current (incomplete) candle

    def to_dict(self) -> Dict[str, float]:
        return {
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "timestamp": self.timestamp,
        }


@dataclass
class KlineBuffer:
    """
    Thread-safe buffer for OHLCV data for one symbol.

    Maintains a rolling window of candles (max_candles) for indicator
    computation. New candles are appended; old ones are evicted.
    """
    symbol: str
    interval: str
    candles: List[OHLCVBar] = field(default_factory=list)
    max_candles: int = 500

    def append(self, bar: OHLCVBar) -> None:
        """Add a new candle, evicting oldest if over capacity."""
        self.candles.append(bar)
        if len(self.candles) > self.max_candles:
            self.candles = self.candles[-self.max_candles:]

    def update_current(self, bar: OHLCVBar) -> None:
        """
        Update the current (incomplete) candle in-place.
        If the bar timestamp is newer, close the old one and start a new.
        """
        if self.candles and self.candles[-1].timestamp == bar.timestamp:
            # Update in-place
            self.candles[-1] = bar
        else:
            # Close previous candle and start new one
            if self.candles:
                self.candles[-1].is_closed = True
            self.append(bar)

    def to_dict_list(self) -> List[Dict[str, float]]:
        """Export as list of dicts (compatible with indicators module)."""
        return [c.to_dict() for c in self.candles]

    @property
    def latest(self) -> Optional[OHLCVBar]:
        return self.candles[-1] if self.candles else None

    @property
    def latest_closed(self) -> Optional[OHLCVBar]:
        """Get the most recent CLOSED candle (for signal evaluation)."""
        closed = [c for c in self.candles if c.is_closed]
        return closed[-1] if closed else None

    def __len__(self) -> int:
        return len(self.candles)


# ---------------------------------------------------------------------------
# MarketDataEngine
# ---------------------------------------------------------------------------

class MarketDataEngine:
    """
    Async engine for fetching and streaming market data.

    Manages REST API requests for historical data and WebSocket connections
    for real-time updates. All data is normalized and stored in KlineBuffers.

    CONCURRENCY:
        - Uses asyncio for non-blocking I/O.
        - WebSocket reconnect with exponential backoff.
        - Thread-safe buffer via asyncio.Lock per symbol.
    """

    def __init__(
        self,
        symbols: List[str],
        intervals: Optional[List[str]] = None,
        primary_interval: str = "5m",
        max_klines_per_request: int = 500,
        ws_reconnect_delay: float = 5.0,
        testnet: bool = True,
        rest_base_url: str = "https://testnet.binance.vision",
        ws_base_url: str = "wss://testnet.binance.vision/ws",
    ):
        """
        Initialize the market data engine.

        Args:
            symbols: Trading pairs to track.
            intervals: Kline intervals to subscribe to (default: ["1m", "5m", "1h"]).
            primary_interval: The main interval for strategy decisions.
            max_klines_per_request: Max candles per REST request.
            ws_reconnect_delay: Initial reconnect delay in seconds.
            testnet: Use Binance testnet if True.
            rest_base_url: Override REST API base URL.
            ws_base_url: Override WebSocket base URL.
        """
        self.symbols = symbols
        self.intervals = intervals or ["1m", "5m", "15m", "1h"]
        self.primary_interval = primary_interval
        self.max_klines_per_request = max_klines_per_request
        self.ws_reconnect_delay = ws_reconnect_delay
        self.testnet = testnet
        self.rest_base_url = rest_base_url
        self.ws_base_url = ws_base_url

        # Buffers: symbol → interval → KlineBuffer
        self.buffers: Dict[str, Dict[str, KlineBuffer]] = defaultdict(dict)

        # Locks for thread-safe buffer access
        self._locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

        # Internal state
        self._running = False
        self._ws_connection = None
        self._ws_task: Optional[asyncio.Task] = None
        self._session: Optional[aiohttp.ClientSession] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """
        Start the data engine: fetch historical data, then open WebSocket.

        Call this once before entering the main loop.
        """
        self._running = True
        self._session = aiohttp.ClientSession()

        # Initialize buffers
        for symbol in self.symbols:
            for interval in self.intervals:
                self.buffers[symbol][interval] = KlineBuffer(
                    symbol=symbol,
                    interval=interval,
                    max_candles=self.max_klines_per_request,
                )

        # Fetch historical data for warm-up
        logger.info(f"Fetching historical klines for {len(self.symbols)} symbols...")
        for symbol in self.symbols:
            for interval in self.intervals:
                await self._fetch_historical(symbol, interval)

        # Start WebSocket stream
        logger.info("Starting WebSocket stream...")
        self._ws_task = asyncio.create_task(self._websocket_loop())

        logger.info("MarketDataEngine started")

    async def stop(self) -> None:
        """Gracefully shut down the data engine."""
        self._running = False

        if self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass

        if self._session:
            await self._session.close()

        logger.info("MarketDataEngine stopped")

    # ------------------------------------------------------------------
    # REST: Historical Data
    # ------------------------------------------------------------------

    async def _fetch_historical(self, symbol: str, interval: str) -> None:
        """
        Fetch historical klines from Binance REST API.

        Binance endpoint: GET /api/v3/klines
        Params: symbol, interval, limit

        Response format (per candle):
            [open_time, open, high, low, close, volume, close_time,
             quote_asset_volume, num_trades, taker_buy_base, taker_buy_quote, ignore]
        """
        url = f"{self.rest_base_url}/api/v3/klines"
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": self.max_klines_per_request,
        }

        try:
            async with self._session.get(url, params=params, timeout=30) as resp:
                if resp.status != 200:
                    logger.error(f"Failed to fetch {symbol} {interval}: HTTP {resp.status}")
                    return

                data = await resp.json()

                if not isinstance(data, list):
                    logger.error(f"Unexpected response for {symbol} {interval}: {data}")
                    return

                buffer = self.buffers[symbol][interval]
                buffer.candles.clear()

                for row in data:
                    bar = OHLCVBar(
                        open=float(row[1]),
                        high=float(row[2]),
                        low=float(row[3]),
                        close=float(row[4]),
                        volume=float(row[5]),
                        timestamp=datetime.fromtimestamp(
                            row[0] / 1000.0, tz=timezone.utc
                        ).isoformat(),
                        is_closed=True,
                    )
                    buffer.candles.append(bar)

                # Mark the last candle as potentially incomplete (current)
                if buffer.candles:
                    buffer.candles[-1].is_closed = False

                logger.info(
                    f"Fetched {len(buffer.candles)} {interval} candles for {symbol}"
                )

        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching {symbol} {interval}")
        except Exception as e:
            logger.exception(f"Error fetching {symbol} {interval}: {e}")

    async def fetch_recent(
        self, symbol: str, interval: str, limit: int = 100
    ) -> List[Dict[str, float]]:
        """
        Fetch recent klines on-demand (for re-syncing after disconnection).

        Args:
            symbol: Trading pair.
            interval: Kline interval.
            limit: Number of candles to fetch.

        Returns:
            List of OHLCV dicts.
        """
        url = f"{self.rest_base_url}/api/v3/klines"
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": limit,
        }

        try:
            async with self._session.get(url, params=params, timeout=15) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
                result = []
                for row in data:
                    result.append({
                        "open": float(row[1]),
                        "high": float(row[2]),
                        "low": float(row[3]),
                        "close": float(row[4]),
                        "volume": float(row[5]),
                        "timestamp": datetime.fromtimestamp(
                            row[0] / 1000.0, tz=timezone.utc
                        ).isoformat(),
                    })
                return result
        except Exception:
            return []

    # ------------------------------------------------------------------
    # WebSocket: Real-Time Stream
    # ------------------------------------------------------------------

    async def _websocket_loop(self) -> None:
        """
        Maintain a persistent WebSocket connection with auto-reconnect.

        Binance WebSocket stream format:
        wss://stream.binance.com:9443/ws/<stream_names>

        Stream name for klines: <symbol>@kline_<interval>
        Example: btcusdt@kline_5m

        Reconnect strategy:
            - On first disconnect: wait ws_reconnect_delay seconds.
            - On subsequent disconnects: exponential backoff (×2 each time).
            - Cap at 60 seconds.
        """
        stream_names = []
        for symbol in self.symbols:
            for interval in self.intervals:
                stream_names.append(f"{symbol.lower()}@kline_{interval}")

        ws_url = f"{self.ws_base_url}/{'/'.join(stream_names)}"
        reconnect_delay = self.ws_reconnect_delay

        while self._running:
            try:
                async with self._session.ws_connect(ws_url) as ws:
                    self._ws_connection = ws
                    logger.info(f"WebSocket connected: {len(stream_names)} streams")
                    reconnect_delay = self.ws_reconnect_delay  # Reset on successful connect

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await self._handle_ws_message(json.loads(msg.data))
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            logger.error(f"WebSocket error: {ws.exception()}")
                            break

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"WebSocket disconnected: {e}. Reconnecting in {reconnect_delay}s...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60.0)

        logger.info("WebSocket loop exited")

    async def _handle_ws_message(self, msg: Dict[str, Any]) -> None:
        """
        Process an incoming WebSocket kline message.

        Binance kline stream message format:
        {
            "e": "kline",
            "E": 123456789,       # Event time
            "s": "BTCUSDT",       # Symbol
            "k": {
                "t": 123456789,   # Kline start time (ms)
                "T": 123456999,   # Kline close time (ms)
                "s": "BTCUSDT",
                "i": "5m",        # Interval
                "o": "100000.00", # Open
                "h": "101000.00", # High
                "l": "99000.00",  # Low
                "c": "100500.00", # Close
                "v": "123.45",    # Volume
                "x": false,       # Is this kline closed?
                ...
            }
        }
        """
        if msg.get("e") != "kline":
            return

        kline = msg["k"]
        symbol = kline["s"]
        interval = kline["i"]

        if symbol not in self.buffers or interval not in self.buffers[symbol]:
            return

        bar = OHLCVBar(
            open=float(kline["o"]),
            high=float(kline["h"]),
            low=float(kline["l"]),
            close=float(kline["c"]),
            volume=float(kline["v"]),
            timestamp=datetime.fromtimestamp(
                kline["t"] / 1000.0, tz=timezone.utc
            ).isoformat(),
            is_closed=kline["x"],
        )

        async with self._locks[symbol]:
            buffer = self.buffers[symbol][interval]
            buffer.update_current(bar)

    # ------------------------------------------------------------------
    # Public Accessors
    # ------------------------------------------------------------------

    async def get_ohlcv(
        self, symbol: str, interval: Optional[str] = None
    ) -> List[Dict[str, float]]:
        """
        Get the current OHLCV buffer for a symbol as a list of dicts.

        Args:
            symbol: Trading pair.
            interval: Kline interval (default: primary_interval).

        Returns:
            List of OHLCV dicts suitable for indicator computation.
        """
        if interval is None:
            interval = self.primary_interval

        async with self._locks[symbol]:
            buffer = self.buffers.get(symbol, {}).get(interval)
            if buffer is None:
                return []
            return buffer.to_dict_list()

    async def get_latest_price(self, symbol: str) -> float:
        """Get the most recent close price for a symbol."""
        async with self._locks[symbol]:
            buffer = self.buffers.get(symbol, {}).get(self.primary_interval)
            if buffer and buffer.latest:
                return buffer.latest.close
            return 0.0

    async def get_latest_bar(
        self, symbol: str, interval: Optional[str] = None
    ) -> Optional[OHLCVBar]:
        """Get the most recent OHLCVBar."""
        if interval is None:
            interval = self.primary_interval
        async with self._locks[symbol]:
            buffer = self.buffers.get(symbol, {}).get(interval)
            return buffer.latest if buffer else None

    @property
    def is_connected(self) -> bool:
        """True if the WebSocket is currently connected."""
        return self._ws_connection is not None and not self._ws_connection.closed


# Need this for type annotation
import aiohttp
