"""
================================================================================
EXCHANGE CLIENT — Binance REST & WebSocket API Wrapper
================================================================================

Provides async methods for interacting with the Binance exchange. All API
calls go through this module — never call the Binance API directly.

RESPONSIBILITIES:
    - REST: Place/cancel orders, query balances, fetch order status.
    - WebSocket: Listen for user data stream (order fills, balance updates).
    - Rate limiting: Enforce requests-per-second limits.
    - Error handling: Retry on transient failures, classify errors.

SECURITY:
    - API keys are NEVER logged.
    - All outbound requests are signed with HMAC-SHA256.
    - Timestamp synchronization with exchange server (recv_window).

CONCURRENCY:
    - Async throughout (aiohttp).
    - Semaphore-based rate limiting.
    - Order dispatch is serialized per symbol to prevent race conditions.
================================================================================
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import math
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Any, Callable, Dict, List, Optional, Tuple

import aiohttp

logger = logging.getLogger("execution.exchange_client")


# ---------------------------------------------------------------------------
# Rate Limiter (Token Bucket)
# ---------------------------------------------------------------------------

class AsyncTokenBucket:
    """
    Async token-bucket rate limiter enforcing a true requests-per-second cap.
    """

    def __init__(self, rate: float, capacity: Optional[float] = None):
        if rate <= 0:
            raise ValueError(f"rate must be > 0, got {rate}")
        self.rate = float(rate)
        self.capacity = float(capacity if capacity is not None else max(1.0, rate))
        self._tokens = self.capacity
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: float = 1.0) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._last_refill
                if elapsed > 0:
                    self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
                    self._last_refill = now
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                wait_time = (tokens - self._tokens) / self.rate
            await asyncio.sleep(wait_time)

    async def __aenter__(self) -> AsyncTokenBucket:
        await self.acquire(1.0)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


# ---------------------------------------------------------------------------
# Exchange Symbol Rules (Precision & Filters)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SymbolInfo:
    """Normalized Binance symbol trading filters (PRICE_FILTER, LOT_SIZE, NOTIONAL)."""

    symbol: str
    base_asset: str
    quote_asset: str
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal
    min_notional: Decimal

    @classmethod
    def from_exchange_filter(cls, raw: Dict[str, Any]) -> SymbolInfo:
        symbol = raw["symbol"].upper()
        base_asset = raw.get("baseAsset", symbol.replace("USDT", ""))
        quote_asset = raw.get("quoteAsset", "USDT")
        tick_size = Decimal("0.00001")
        step_size = Decimal("1")
        min_qty = Decimal("0")
        min_notional = Decimal("5.0")

        for f in raw.get("filters", []):
            ftype = f.get("filterType")
            if ftype == "PRICE_FILTER" and f.get("tickSize"):
                tick_size = Decimal(str(f["tickSize"])).normalize()
            elif ftype == "LOT_SIZE":
                if f.get("stepSize"):
                    step_size = Decimal(str(f["stepSize"])).normalize()
                if f.get("minQty"):
                    min_qty = Decimal(str(f["minQty"])).normalize()
            elif ftype in ("NOTIONAL", "MIN_NOTIONAL") and f.get("minNotional"):
                min_notional = Decimal(str(f["minNotional"])).normalize()

        return cls(
            symbol=symbol,
            base_asset=base_asset,
            quote_asset=quote_asset,
            tick_size=tick_size,
            step_size=step_size,
            min_qty=min_qty,
            min_notional=min_notional,
        )

    @classmethod
    def default_for(cls, symbol: str) -> SymbolInfo:
        """Reasonable fallback defaults when offline / in unit tests."""
        sym = symbol.upper()
        defaults = {
            "BTCUSDT": ("BTC", "USDT", "0.01", "0.00001", "0.00001", "5.0"),
            "ETHUSDT": ("ETH", "USDT", "0.01", "0.0001", "0.0001", "5.0"),
            "SOLUSDT": ("SOL", "USDT", "0.01", "0.01", "0.01", "5.0"),
            "BNBUSDT": ("BNB", "USDT", "0.1", "0.001", "0.001", "5.0"),
            "DOGEUSDT": ("DOGE", "USDT", "0.00001", "1", "1", "1.0"),
        }
        base, quote, tick, step, min_q, min_n = defaults.get(
            sym, (sym.replace("USDT", ""), "USDT", "0.0001", "0.001", "0.001", "5.0")
        )
        return cls(
            symbol=sym,
            base_asset=base,
            quote_asset=quote,
            tick_size=Decimal(tick),
            step_size=Decimal(step),
            min_qty=Decimal(min_q),
            min_notional=Decimal(min_n),
        )

    def round_price(self, price: float) -> float:
        """Round price to the nearest valid tick_size."""
        if price <= 0 or self.tick_size <= 0:
            return 0.0
        d = Decimal(str(price))
        rounded = (d / self.tick_size).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * self.tick_size
        return float(rounded)

    def floor_qty(self, qty: float) -> float:
        """Floor quantity to a valid multiple of step_size (never oversells)."""
        if qty <= 0 or self.step_size <= 0:
            return 0.0
        d = Decimal(str(qty))
        floored = (d / self.step_size).quantize(Decimal("1"), rounding=ROUND_DOWN) * self.step_size
        return float(floored)

    def is_valid_order(self, price: float, qty: float) -> bool:
        """Check min_qty and min_notional constraints."""
        if price <= 0 or qty <= 0:
            return False
        if Decimal(str(qty)) < self.min_qty:
            return False
        if Decimal(str(price)) * Decimal(str(qty)) < self.min_notional:
            return False
        return True


# ---------------------------------------------------------------------------
# Data Structures
# ---------------------------------------------------------------------------

@dataclass
class OrderRequest:
    """Normalized order request sent to the exchange."""
    symbol: str
    side: str  # BUY or SELL
    order_type: str  # LIMIT, MARKET, STOP_LOSS, STOP_LOSS_LIMIT, TAKE_PROFIT
    quantity: float
    price: Optional[float] = None
    stop_price: Optional[float] = None
    time_in_force: str = "GTC"
    client_order_id: Optional[str] = None
    new_client_order_id: Optional[str] = None  # For modification
    reduce_only: bool = False

    def validate(self) -> Tuple[bool, str]:
        """Validate order parameters before dispatch."""
        if self.order_type in ("LIMIT", "STOP_LOSS_LIMIT", "TAKE_PROFIT_LIMIT") and self.price is None:
            return False, f"Price required for {self.order_type} order"
        if self.order_type in (
            "STOP_LOSS", "STOP_LOSS_LIMIT", "STOP_MARKET",
            "TAKE_PROFIT", "TAKE_PROFIT_LIMIT", "TAKE_PROFIT_MARKET",
        ) and self.stop_price is None:
            return False, f"Stop price required for {self.order_type} order"
        if self.quantity <= 0:
            return False, f"Quantity must be positive, got {self.quantity}"
        if self.price is not None and self.price <= 0:
            return False, f"Price must be positive, got {self.price}"
        return True, "ok"


@dataclass
class OrderResponse:
    """Normalized order response from the exchange."""
    order_id: str  # Our internal ID
    exchange_order_id: str  # Binance order ID
    client_order_id: str
    symbol: str
    side: str
    order_type: str
    status: str
    price: float
    quantity: float
    executed_qty: float
    cummulative_quote_qty: float
    created_at: str
    updated_at: str
    raw_response: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BalanceInfo:
    """Account balance for a single asset."""
    asset: str
    free: float
    locked: float

    @property
    def total(self) -> float:
        return self.free + self.locked


# ---------------------------------------------------------------------------
# ExchangeClient
# ---------------------------------------------------------------------------

class ExchangeClient:
    """
    Async Binance API client with rate limiting and retry logic.

    USAGE:
        client = ExchangeClient(api_key="...", api_secret="...", testnet=True)
        order = await client.place_order(OrderRequest(
            symbol="BTCUSDT", side="BUY", order_type="LIMIT",
            price=97000.0, quantity=0.001,
        ))
        await client.close()
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        testnet: bool = True,
        market: str = "spot",
        recv_window: int = 5000,
        rate_limit_rps: float = 10.0,
        max_retries: int = 3,
    ):
        """
        Initialize the exchange client.

        Args:
            api_key: Binance API key.
            api_secret: Binance API secret.
            testnet: Use testnet endpoints if True.
            market: "spot" or "futures" — determines API base URL and endpoints.
            recv_window: Timestamp tolerance in ms.
            rate_limit_rps: Max requests per second.
            max_retries: Max retries on transient failures.
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        self.market = market
        self.recv_window = recv_window
        self.rate_limit_rps = rate_limit_rps
        self.max_retries = max_retries

        # Base URLs for spot vs futures
        if market == "futures":
            self._rest_base = (
                "https://testnet.binancefuture.com" if testnet
                else "https://fapi.binance.com"
            )
            self._ws_base = (
                "wss://stream.binancefuture.com/ws" if testnet
                else "wss://fstream.binance.com/ws"
            )
            self._api_prefix = "/fapi/v1"
            self._user_stream_endpoint = "/fapi/v1/listenKey"
        else:
            self._rest_base = (
                "https://testnet.binance.vision" if testnet
                else "https://api.binance.com"
            )
            self._ws_base = (
                "wss://stream.testnet.binance.vision/ws" if testnet
                else "wss://stream.binance.com:9443/ws"
            )
            self._api_prefix = "/api/v3"
            self._user_stream_endpoint = "/api/v3/userDataStream"

        self._session: Optional[aiohttp.ClientSession] = None
        self._rate_limiter = AsyncTokenBucket(rate=rate_limit_rps, capacity=max(1.0, rate_limit_rps))
        self._order_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._time_offset_ms: float = 0.0  # server time – local time (ms)
        self._symbol_info_cache: Dict[str, SymbolInfo] = {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Initialize HTTP session and sync clock with Binance server."""
        self._session = aiohttp.ClientSession()
        await self._sync_server_time()
        logger.info(f"ExchangeClient started ({'testnet' if self.testnet else 'live'})"
                     f"{f', offset={self._time_offset_ms:.0f}ms' if abs(self._time_offset_ms) > 100 else ''}")

    async def _sync_server_time(self) -> None:
        """Fetch Binance server time, compute offset from local clock."""
        if self._session is None:
            return
        try:
            url = f"{self._rest_base}{self._api_prefix}/time"
            async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self._time_offset_ms = data["serverTime"] - int(time.time() * 1000)
        except Exception:
            pass

    async def close(self) -> None:
        """Close the HTTP session."""
        if self._session:
            await self._session.close()
        logger.info("ExchangeClient closed")

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def _sign(self, params: Dict[str, Any]) -> str:
        """Generate HMAC-SHA256 signature for the request."""
        query_string = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        return hmac.new(
            self.api_secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    async def _signed_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Make a signed request to the Binance API.

        Args:
            method: HTTP method (GET, POST, DELETE).
            endpoint: API endpoint (e.g., f"{self._api_prefix}/order").
            params: Query parameters.

        Returns:
            Parsed JSON response.

        Raises:
            BinanceAPIError: On API-level errors.
            ExchangeConnectionError: On network errors.
        """
        if params is None:
            params = {}

        params["timestamp"] = int(time.time() * 1000 + self._time_offset_ms)
        params["recvWindow"] = self.recv_window
        params["signature"] = self._sign(params)

        query_string = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        full_url = f"{self._rest_base}{endpoint}?{query_string}"
        headers = {"X-MBX-APIKEY": self.api_key}

        async with self._rate_limiter:
            for attempt in range(self.max_retries):
                try:
                    timeout = aiohttp.ClientTimeout(total=10)
                    if method == "GET":
                        async with self._session.get(full_url, headers=headers, timeout=timeout) as resp:
                            data = await resp.json()
                    elif method == "POST":
                        async with self._session.post(full_url, headers=headers, timeout=timeout) as resp:
                            data = await resp.json()
                    elif method == "DELETE":
                        async with self._session.delete(full_url, headers=headers, timeout=timeout) as resp:
                            data = await resp.json()
                    else:
                        raise ValueError(f"Unsupported HTTP method: {method}")

                    if resp.status == 429:
                        await asyncio.sleep(2 ** attempt)
                        continue

                    if resp.status >= 400:
                        error_msg = data.get("msg", str(data))
                        error_code = data.get("code", 0)

                        # recvWindow / timestamp: re-sync and retry once
                        if error_code == -1021 or "Timestamp" in error_msg or "recvWindow" in error_msg:
                            logger.warning(
                                f"Timestamp rejected (offset={self._time_offset_ms:.0f}ms). "
                                f"Re-syncing server time..."
                            )
                            await self._sync_server_time()
                            params.pop("signature", None)
                            params["timestamp"] = int(time.time() * 1000 + self._time_offset_ms)
                            params["signature"] = self._sign(params)
                            query_string = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
                            full_url = f"{self._rest_base}{endpoint}?{query_string}"
                            continue

                        logger.error(f"Binance API error [{resp.status}]: {error_msg}")
                        if resp.status >= 500:
                            await asyncio.sleep(2 ** attempt)
                            continue
                        raise BinanceAPIError(resp.status, error_msg, data)

                    return data

                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    raise ExchangeConnectionError(f"Connection failed: {e}")

    # ------------------------------------------------------------------
    # Order Management
    # ------------------------------------------------------------------

    async def place_order(self, request: OrderRequest) -> OrderResponse:
        """
        Place a new order on the exchange.

        Args:
            request: Normalized order request.

        Returns:
            OrderResponse with exchange order ID and status.
        """
        valid, error = request.validate()
        if not valid:
            raise ValueError(f"Invalid order: {error}")

        # Serialize orders per symbol to prevent race conditions
        async with self._order_locks[request.symbol]:
            if request.client_order_id is None:
                request.client_order_id = f"co_{uuid.uuid4().hex[:16]}"

            params = {
                "symbol": request.symbol.upper(),
                "side": request.side.upper(),
                "type": request.order_type.upper(),
                "quantity": request.quantity,
                "newClientOrderId": request.client_order_id,
                "newOrderRespType": "RESULT" if self.market == "futures" else "FULL",
            }

            if request.price is not None:
                params["price"] = request.price
            if request.stop_price is not None:
                params["stopPrice"] = request.stop_price
            if request.order_type in ("LIMIT", "STOP_LOSS_LIMIT", "TAKE_PROFIT_LIMIT"):
                params["timeInForce"] = request.time_in_force
            if request.reduce_only and self.market == "futures":
                params["reduceOnly"] = "true"

            logger.info(
                f"Placing order: {request.symbol} {request.side} "
                f"{request.order_type} qty={request.quantity}"
                + (f" @ {request.price}" if request.price else "")
                + (" [reduceOnly]" if request.reduce_only and self.market == "futures" else "")
            )

            response = await self._signed_request("POST", f"{self._api_prefix}/order", params)

            return OrderResponse(
                order_id=request.client_order_id,
                exchange_order_id=str(response.get("orderId", "")),
                client_order_id=request.client_order_id,
                symbol=response.get("symbol", request.symbol),
                side=response.get("side", request.side),
                order_type=response.get("type", request.order_type),
                status=response.get("status", "UNKNOWN"),
                price=float(response.get("avgPrice") or response.get("price") or 0),
                quantity=float(response.get("origQty", 0)),
                executed_qty=float(response.get("executedQty", 0)),
                cummulative_quote_qty=float(
                    response.get("cumQuote") or response.get("cummulativeQuoteQty") or 0
                ),
                created_at=datetime.now(timezone.utc).isoformat(),
                updated_at=datetime.now(timezone.utc).isoformat(),
                raw_response=response,
            )

    async def cancel_order(self, symbol: str, order_id: Optional[str] = None,
                           client_order_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Cancel an order by exchange order ID or client order ID.

        Args:
            symbol: Trading pair.
            order_id: Binance order ID.
            client_order_id: Our client order ID.

        Returns:
            Cancellation response.
        """
        if not order_id and not client_order_id:
            raise ValueError("Either order_id or client_order_id is required")

        params = {"symbol": symbol.upper()}
        if order_id:
            params["orderId"] = order_id
        if client_order_id:
            params["origClientOrderId"] = client_order_id

        return await self._signed_request("DELETE", f"{self._api_prefix}/order", params)

    async def cancel_all_orders(self, symbol: str) -> List[Dict[str, Any]]:
        """Cancel all open orders for a symbol."""
        params = {"symbol": symbol.upper()}
        return await self._signed_request("DELETE", f"{self._api_prefix}/openOrders", params)

    async def get_order_status(self, symbol: str, order_id: Optional[str] = None,
                               client_order_id: Optional[str] = None) -> Dict[str, Any]:
        """Query the status of an order."""
        params = {"symbol": symbol.upper()}
        if order_id:
            params["orderId"] = order_id
        if client_order_id:
            params["origClientOrderId"] = client_order_id

        return await self._signed_request("GET", f"{self._api_prefix}/order", params)

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all currently open orders."""
        params = {}
        if symbol:
            params["symbol"] = symbol.upper()
        return await self._signed_request("GET", f"{self._api_prefix}/openOrders", params)

    async def get_my_trades(
        self,
        symbol: str,
        limit: int = 50,
        start_time: Optional[int] = None,
        from_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch recent account trades (fills) for a symbol (Spot myTrades / Futures userTrades)."""
        params: Dict[str, Any] = {"symbol": symbol.upper(), "limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        if from_id is not None:
            params["fromId"] = from_id
        endpoint = "/fapi/v1/userTrades" if self.market == "futures" else f"{self._api_prefix}/myTrades"
        return await self._signed_request("GET", endpoint, params)

    # ------------------------------------------------------------------
    # Futures Leverage, Margin Mode & Positions
    # ------------------------------------------------------------------

    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """Set isolated/crossed leverage for a USDT-M Futures symbol."""
        if self.market != "futures":
            return {}
        params = {"symbol": symbol.upper(), "leverage": max(1, int(leverage))}
        return await self._signed_request("POST", "/fapi/v1/leverage", params)

    async def set_margin_type(self, symbol: str, margin_type: str = "ISOLATED") -> Dict[str, Any]:
        """Set margin type (ISOLATED/CROSSED) for a USDT-M Futures symbol."""
        if self.market != "futures":
            return {}
        params = {"symbol": symbol.upper(), "marginType": margin_type.upper()}
        try:
            return await self._signed_request("POST", "/fapi/v1/marginType", params)
        except BinanceAPIError as e:
            if "-4046" in str(e) or "No need to change margin type" in str(e):
                return {"msg": "already_set"}
            raise

    async def get_futures_positions(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch open USDT-M Futures positionRisk entries."""
        if self.market != "futures":
            return []
        params = {"symbol": symbol.upper()} if symbol else {}
        return await self._signed_request("GET", "/fapi/v2/positionRisk", params)

    # ------------------------------------------------------------------
    # User Data Stream (listenKey)
    # ------------------------------------------------------------------

    async def create_listen_key(self) -> str:
        """Create a Binance User Data Stream listenKey."""
        url = f"{self._rest_base}{self._user_stream_endpoint}"
        headers = {"X-MBX-APIKEY": self.api_key}
        timeout = aiohttp.ClientTimeout(total=10)
        async with self._rate_limiter:
            async with self._session.post(url, headers=headers, timeout=timeout) as resp:
                data = await resp.json()
                if resp.status >= 400:
                    raise BinanceAPIError(resp.status, data.get("msg", str(data)), data)
                return data["listenKey"]

    async def keepalive_listen_key(self, listen_key: str) -> None:
        """Extend the validity of a listenKey (call every ~30 minutes)."""
        url = f"{self._rest_base}{self._user_stream_endpoint}"
        headers = {"X-MBX-APIKEY": self.api_key}
        params = {"listenKey": listen_key}
        timeout = aiohttp.ClientTimeout(total=10)
        async with self._rate_limiter:
            async with self._session.put(url, headers=headers, params=params, timeout=timeout) as resp:
                if resp.status >= 400:
                    data = await resp.json()
                    raise BinanceAPIError(resp.status, data.get("msg", str(data)), data)

    async def close_listen_key(self, listen_key: str) -> None:
        """Close an active listenKey on shutdown."""
        url = f"{self._rest_base}{self._user_stream_endpoint}"
        headers = {"X-MBX-APIKEY": self.api_key}
        params = {"listenKey": listen_key}
        timeout = aiohttp.ClientTimeout(total=10)
        try:
            async with self._session.delete(url, headers=headers, params=params, timeout=timeout):
                pass
        except Exception:
            pass

    def get_user_stream_ws_url(self, listen_key: str) -> str:
        """Build the WebSocket URL for the User Data Stream."""
        return f"{self._ws_base}/{listen_key}"

    # ------------------------------------------------------------------
    # Account Information
    # ------------------------------------------------------------------

    async def get_balances(self) -> List[BalanceInfo]:
        """Get current account balances (Spot /api/v3/account or Futures /fapi/v2/balance)."""
        if self.market == "futures":
            rows = await self._signed_request("GET", "/fapi/v2/balance")
            balances = []
            for b in rows:
                total = float(b.get("balance", 0.0))
                free = float(b.get("availableBalance", 0.0))
                locked = max(0.0, total - free)
                if total > 0 or free > 0:
                    balances.append(BalanceInfo(asset=b["asset"], free=free, locked=locked))
            return balances

        response = await self._signed_request("GET", f"{self._api_prefix}/account")
        balances = []
        for b in response.get("balances", []):
            free = float(b["free"])
            locked = float(b["locked"])
            if free > 0 or locked > 0:
                balances.append(BalanceInfo(
                    asset=b["asset"],
                    free=free,
                    locked=locked,
                ))
        return balances

    async def get_asset_balance(self, asset: str) -> BalanceInfo:
        """Get balance for a specific asset."""
        balances = await self.get_balances()
        for b in balances:
            if b.asset == asset.upper():
                return b
        return BalanceInfo(asset=asset.upper(), free=0.0, locked=0.0)

    # ------------------------------------------------------------------
    # Market Data & Symbol Info Cache
    # ------------------------------------------------------------------

    async def get_ticker_price(self, symbol: str) -> float:
        """Get the current price for a symbol (no auth needed)."""
        url = f"{self._rest_base}{self._api_prefix}/ticker/price"
        params = {"symbol": symbol.upper()}
        timeout = aiohttp.ClientTimeout(total=10)
        async with self._session.get(url, params=params, timeout=timeout) as resp:
            data = await resp.json()
            return float(data["price"])

    async def get_exchange_info(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Get exchange trading rules and symbol information."""
        url = f"{self._rest_base}{self._api_prefix}/exchangeInfo"
        params = {}
        if symbol:
            params["symbol"] = symbol.upper()
        timeout = aiohttp.ClientTimeout(total=10)
        async with self._session.get(url, params=params, timeout=timeout) as resp:
            return await resp.json()

    async def load_symbol_info(self, symbols: List[str]) -> Dict[str, SymbolInfo]:
        """Fetch and cache SymbolInfo filters for the requested symbols."""
        for sym in symbols:
            key = sym.upper()
            try:
                info = await self.get_exchange_info(key)
                for s in info.get("symbols", []):
                    if s.get("symbol", "").upper() == key:
                        self._symbol_info_cache[key] = SymbolInfo.from_exchange_filter(s)
                        break
            except Exception as e:
                logger.warning(f"Failed to load exchangeInfo for {key}: {e}. Using defaults.")
                self._symbol_info_cache.setdefault(key, SymbolInfo.default_for(key))
        return dict(self._symbol_info_cache)

    def get_symbol_info(self, symbol: str) -> SymbolInfo:
        """Return cached SymbolInfo for a symbol (falling back to safe defaults)."""
        key = symbol.upper()
        if key not in self._symbol_info_cache:
            self._symbol_info_cache[key] = SymbolInfo.default_for(key)
        return self._symbol_info_cache[key]

    # ------------------------------------------------------------------
    # Server Time Sync
    # ------------------------------------------------------------------

    async def get_server_time(self) -> int:
        """Get the exchange server time in milliseconds."""
        url = f"{self._rest_base}{self._api_prefix}/time"
        timeout = aiohttp.ClientTimeout(total=5)
        async with self._session.get(url, timeout=timeout) as resp:
            data = await resp.json()
            return data["serverTime"]


# ---------------------------------------------------------------------------
# Custom Exceptions
# ---------------------------------------------------------------------------

class BinanceAPIError(Exception):
    """Raised when the Binance API returns an error."""
    def __init__(self, status_code: int, message: str, raw: Dict[str, Any] = None):
        self.status_code = status_code
        self.message = message
        self.raw = raw or {}
        super().__init__(f"Binance API error [{status_code}]: {message}")


class ExchangeConnectionError(Exception):
    """Raised when network communication with the exchange fails."""
    pass


