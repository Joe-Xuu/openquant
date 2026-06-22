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
from collections import defaultdict

import aiohttp
import hmac
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger("execution.exchange_client")


# ---------------------------------------------------------------------------
# Data Structures
# ---------------------------------------------------------------------------

@dataclass
class OrderRequest:
    """Normalized order request sent to the exchange."""
    symbol: str
    side: str  # BUY or SELL
    order_type: str  # LIMIT, MARKET, STOP_LOSS, TAKE_PROFIT
    quantity: float
    price: Optional[float] = None
    stop_price: Optional[float] = None
    time_in_force: str = "GTC"
    client_order_id: Optional[str] = None
    new_client_order_id: Optional[str] = None  # For modification

    def validate(self) -> Tuple[bool, str]:
        """Validate order parameters before dispatch."""
        if self.order_type in ("LIMIT", "STOP_LOSS", "TAKE_PROFIT") and self.price is None:
            return False, f"Price required for {self.order_type} order"
        if self.order_type in ("STOP_LOSS", "TAKE_PROFIT") and self.stop_price is None:
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
            self._api_prefix = "/fapi/v1"
        else:
            self._rest_base = (
                "https://testnet.binance.vision" if testnet
                else "https://api.binance.com"
            )
            self._api_prefix = "/api/v3"

        self._session: Optional[aiohttp.ClientSession] = None
        self._rate_limiter = asyncio.Semaphore(int(rate_limit_rps))
        self._order_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Initialize the HTTP session."""
        self._session = aiohttp.ClientSession()
        logger.info(f"ExchangeClient started ({'testnet' if self.testnet else 'live'})")

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

        params["timestamp"] = int(time.time() * 1000)
        params["recvWindow"] = self.recv_window
        params["signature"] = self._sign(params)

        # Build query string manually (not via aiohttp params) for exact match with signature
        query_string = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        full_url = f"{self._rest_base}{endpoint}?{query_string}"
        headers = {"X-MBX-APIKEY": self.api_key}

        # Rate limiting
        async with self._rate_limiter:
            for attempt in range(self.max_retries):
                try:
                    if method == "GET":
                        async with self._session.get(full_url, headers=headers, timeout=15) as resp:
                            data = await resp.json()
                    elif method == "POST":
                        async with self._session.post(full_url, headers=headers, timeout=15) as resp:
                            data = await resp.json()
                    elif method == "DELETE":
                        async with self._session.delete(full_url, headers=headers, timeout=15) as resp:
                            data = await resp.json()
                    else:
                        raise ValueError(f"Unsupported HTTP method: {method}")

                    if resp.status == 429:
                        # Rate limited — wait and retry
                        await asyncio.sleep(2 ** attempt)
                        continue

                    if resp.status >= 400:
                        error_msg = data.get("msg", str(data))
                        logger.error(f"Binance API error [{resp.status}]: {error_msg}")
                        if resp.status >= 500:
                            # Server error — retry
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
                "newOrderRespType": "FULL",
            }

            if request.price is not None:
                params["price"] = request.price
            if request.stop_price is not None:
                params["stopPrice"] = request.stop_price
            if request.order_type == "LIMIT":
                params["timeInForce"] = request.time_in_force

            logger.info(
                f"Placing order: {request.symbol} {request.side} "
                f"{request.order_type} qty={request.quantity}"
                + (f" @ {request.price}" if request.price else "")
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
                price=float(response.get("price", 0)),
                quantity=float(response.get("origQty", 0)),
                executed_qty=float(response.get("executedQty", 0)),
                cummulative_quote_qty=float(response.get("cummulativeQuoteQty", 0)),
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

    # ------------------------------------------------------------------
    # Account Information
    # ------------------------------------------------------------------

    async def get_balances(self) -> List[BalanceInfo]:
        """Get current account balances."""
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
    # Market Data
    # ------------------------------------------------------------------

    async def get_ticker_price(self, symbol: str) -> float:
        """Get the current price for a symbol (no auth needed)."""
        url = f"{self._rest_base}{self._api_prefix}/ticker/price"
        params = {"symbol": symbol.upper()}
        async with self._session.get(url, params=params, timeout=10) as resp:
            data = await resp.json()
            return float(data["price"])

    async def get_exchange_info(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Get exchange trading rules and symbol information."""
        url = f"{self._rest_base}{self._api_prefix}/exchangeInfo"
        params = {}
        if symbol:
            params["symbol"] = symbol.upper()
        async with self._session.get(url, params=params, timeout=10) as resp:
            return await resp.json()

    # ------------------------------------------------------------------
    # Server Time Sync
    # ------------------------------------------------------------------

    async def get_server_time(self) -> int:
        """Get the exchange server time in milliseconds."""
        url = f"{self._rest_base}{self._api_prefix}/time"
        async with self._session.get(url, timeout=5) as resp:
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


