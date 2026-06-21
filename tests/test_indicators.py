"""Tests for technical indicators: EMA, MACD, ADX, ATR, RSI, Bollinger, volatility."""
import sys
sys.path.insert(0, ".")

import math
from data.indicators import (
    adx, atr, bollinger_bands, compute_all, ema, latest_atr, macd,
    relative_volume, rsi, sma, vwap,
)


def make_ohlcv(n=100, start_price=100000, trend=0.0):
    """Generate synthetic OHLCV with optional trend."""
    import random
    random.seed(42)
    candles = []
    price = start_price
    for i in range(n):
        price += trend * price * 0.001 + random.gauss(0, price * 0.005)
        high = price * (1 + random.uniform(0, 0.01))
        low = price * (1 - random.uniform(0, 0.01))
        candles.append({
            "open": low + random.uniform(0, high - low),
            "high": high, "low": low, "close": price,
            "volume": random.uniform(100, 1000),
        })
    return candles


class TestMovingAverages:
    def test_sma_length(self):
        vals = [10.0, 20.0, 30.0, 40.0, 50.0]
        result = sma(vals, 3)
        assert len(result) == 5

    def test_ema_smoothed(self):
        vals = [100.0] * 20
        result = ema(vals, 10)
        assert abs(result[-1] - 100.0) < 0.01


class TestMACD:
    def test_macd_returns_three_series(self):
        closes = [100.0 + i * 0.1 for i in range(50)]
        macd_line, signal_line, hist = macd(closes)
        assert len(macd_line) == 50
        assert len(signal_line) == 50
        assert len(hist) == 50

    def test_macd_hist_sum(self):
        closes = [100.0 + i for i in range(50)]
        _, _, hist = macd(closes)
        assert hist[-1] != 0


class TestADX:
    def test_adx_returns_tuple(self):
        ohlcv = make_ohlcv(30)
        adx_val, pdi, mdi = adx(ohlcv)
        assert isinstance(adx_val, float)
        assert isinstance(pdi, float)
        assert isinstance(mdi, float)

    def test_adx_non_negative(self):
        ohlcv = make_ohlcv(30)
        adx_val, _, _ = adx(ohlcv)
        assert adx_val >= 0


class TestATR:
    def test_atr_length(self):
        ohlcv = make_ohlcv(30)
        result = atr(ohlcv, 14)
        assert len(result) == 30

    def test_latest_atr(self):
        ohlcv = make_ohlcv(30)
        val = latest_atr(ohlcv, 14)
        assert val > 0


class TestRSI:
    def test_rsi_range(self):
        closes = [100.0 + i * 0.5 for i in range(30)]
        result = rsi(closes, 14)
        assert all(0 <= v <= 100 for v in result[14:])


class TestBollinger:
    def test_bands_ordering(self):
        closes = [100.0 + i * 0.2 for i in range(50)]
        mid, upper, lower = bollinger_bands(closes, 20)
        # Upper > middle > lower at the end
        assert upper[-1] > mid[-1] > lower[-1]


class TestVolume:
    def test_vwap(self):
        ohlcv = make_ohlcv(20)
        v = vwap(ohlcv)
        assert v > 0

    def test_relative_volume(self):
        ohlcv = make_ohlcv(30)
        rv = relative_volume(ohlcv)
        assert rv > 0


class TestComputeAll:
    def test_compute_all_returns_bundle(self):
        ohlcv = make_ohlcv(50)
        bundle = compute_all("BTCUSDT", ohlcv)
        assert bundle.symbol == "BTCUSDT"
        assert bundle.close > 0
        assert bundle.ema_fast > 0
        assert bundle.atr > 0
        assert 0 <= bundle.rsi <= 100
        assert bundle.to_dict()
