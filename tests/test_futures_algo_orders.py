"""
Tests for Binance USDS-M Futures Algo Order API routing (STOP_MARKET -> /fapi/v1/algoOrder).
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from execution.exchange_client import BinanceAPIError, ExchangeClient, OrderRequest


class TestFuturesAlgoOrderRouting:
    @pytest.mark.asyncio
    async def test_stop_market_routes_to_algo_order_endpoint(self):
        client = ExchangeClient(
            api_key="k",
            api_secret="s",
            testnet=True,
            market="futures",
        )
        calls = []

        async def fake_signed(method: str, endpoint: str, params=None):
            calls.append((method, endpoint, dict(params or {})))
            if method == "POST" and endpoint == "/fapi/v1/algoOrder":
                return {
                    "algoId": 987654,
                    "clientAlgoId": params["clientAlgoId"],
                    "symbol": params["symbol"],
                    "side": params["side"],
                    "orderType": params["type"],
                    "algoStatus": "NEW",
                    "triggerPrice": str(params["triggerPrice"]),
                    "quantity": str(params["quantity"]),
                }
            if method == "GET" and endpoint == "/fapi/v1/openOrders":
                return []
            if method == "GET" and endpoint == "/fapi/v1/openAlgoOrders":
                return [{
                    "algoId": 987654,
                    "clientAlgoId": "trend_sl_abc",
                    "symbol": "DOGEUSDT",
                    "side": "BUY",
                    "orderType": "STOP_MARKET",
                    "triggerPrice": "0.06100",
                    "quantity": "1000",
                    "reduceOnly": True,
                    "createTime": 1700000000000,
                }]
            if method == "DELETE" and endpoint == "/fapi/v1/order":
                raise BinanceAPIError(-2011, "Unknown order sent")
            if method == "DELETE" and endpoint == "/fapi/v1/algoOrder":
                return {"algoId": 987654, "code": 200}
            if method == "DELETE" and endpoint in ("/fapi/v1/openOrders", "/fapi/v1/algoOpenOrders"):
                return []
            raise AssertionError(f"Unexpected call: {method} {endpoint}")

        client._signed_request = fake_signed

        # 1. Place STOP_MARKET -> POST /fapi/v1/algoOrder
        req = OrderRequest(
            symbol="DOGEUSDT",
            side="BUY",
            order_type="STOP_MARKET",
            quantity=1000.0,
            stop_price=0.061,
            client_order_id="trend_sl_abc",
            reduce_only=True,
        )
        resp = await client.place_order(req)
        assert resp.exchange_order_id == "987654"
        assert resp.status == "NEW"
        assert calls[0][0] == "POST"
        assert calls[0][1] == "/fapi/v1/algoOrder"
        assert calls[0][2]["algoType"] == "CONDITIONAL"
        assert calls[0][2]["triggerPrice"] == 0.061
        assert calls[0][2]["reduceOnly"] == "true"

        # 2. get_open_orders merges openAlgoOrders
        open_orders = await client.get_open_orders("DOGEUSDT")
        assert len(open_orders) == 1
        assert open_orders[0]["orderId"] == "987654"
        assert open_orders[0]["clientOrderId"] == "trend_sl_abc"
        assert open_orders[0]["_is_algo"] is True

        # 3. cancel_order falls back to DELETE /fapi/v1/algoOrder
        cancel_res = await client.cancel_order("DOGEUSDT", order_id="987654")
        assert cancel_res["algoId"] == 987654

        # 4. cancel_all_orders cancels both openOrders and algoOpenOrders
        calls.clear()
        await client.cancel_all_orders("DOGEUSDT")
        endpoints = [c[1] for c in calls]
        assert "/fapi/v1/openOrders" in endpoints
        assert "/fapi/v1/algoOpenOrders" in endpoints
