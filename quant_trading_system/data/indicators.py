"""
================================================================================
TECHNICAL INDICATORS — Self-Contained Computation Engine
================================================================================

Computes all technical indicators required by the strategy layer from raw
OHLCV data. Implemented here rather than relying on pandas-ta/TA-Lib to
maintain zero external dependency risk for the strategy layer.

SUPPORTED INDICATORS:
    - EMA (Exponential Moving Average)
    - SMA (Simple Moving Average)
    - MACD (Moving Average Convergence Divergence)
    - ADX (Average Directional Index) with +DI/-DI
    - ATR (Average True Range)
    - Bollinger Bands
    - RSI (Relative Strength Index)
    - Volume-weighted average price (VWAP)
    - Volatility (realized, Parkinson, Garman-Klass)

All functions accept List[Dict] (OHLCV format) and return either a scalar
(latest value) or a List[float] (full series).

FORMAT:
    OHLCV dict: {"open": float, "high": float, "low": float, "close": float,
                  "volume": float, "timestamp": str (optional)}
================================================================================
"""

import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Moving Averages
# ---------------------------------------------------------------------------

def ema(values: List[float], period: int) -> List[float]:
    """
    Exponential Moving Average.

    EMA = (price - prev_ema) * (2/(period+1)) + prev_ema

    Args:
        values: Price series (most recent last).
        period: Lookback period.

    Returns:
        EMA series of same length (first period-1 values are SMA).
    """
    if not values or period <= 0 or period > len(values):
        return [sum(values) / len(values)] * len(values) if values else []

    result = [0.0] * len(values)
    multiplier = 2.0 / (period + 1)

    # Seed with SMA
    result[period - 1] = sum(values[:period]) / period
    for i in range(period, len(values)):
        result[i] = (values[i] - result[i - 1]) * multiplier + result[i - 1]

    # Backfill early values with the first computed EMA
    for i in range(period - 1):
        result[i] = result[period - 1]

    return result


def sma(values: List[float], period: int) -> List[float]:
    """Simple Moving Average."""
    if not values or period <= 0:
        return []
    result = []
    for i in range(len(values)):
        window = values[max(0, i - period + 1):i + 1]
        result.append(sum(window) / len(window))
    return result


# ---------------------------------------------------------------------------
# MACD
# ---------------------------------------------------------------------------

def macd(
    closes: List[float],
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> Tuple[List[float], List[float], List[float]]:
    """
    Compute MACD line, signal line, and histogram.

    Returns:
        Tuple of (macd_line, signal_line, histogram) — each a List[float].
    """
    if len(closes) < slow:
        return [], [], []

    ema_fast = ema(closes, fast)
    ema_slow = ema(closes, slow)

    macd_line = [ema_fast[i] - ema_slow[i] for i in range(len(closes))]
    signal_line = ema(macd_line, signal)
    histogram = [macd_line[i] - signal_line[i] for i in range(len(closes))]

    return macd_line, signal_line, histogram


# ---------------------------------------------------------------------------
# ADX (Average Directional Index)
# ---------------------------------------------------------------------------

def adx(
    ohlcv: List[Dict[str, float]],
    period: int = 14,
) -> Tuple[float, float, float]:
    """
    Compute ADX, +DI, -DI using Wilder's smoothing.

    Args:
        ohlcv: OHLCV data (most recent last).
        period: ADX period (typically 14).

    Returns:
        Tuple of (adx, plus_di, minus_di) — latest values as floats.
    """
    if len(ohlcv) < period + 1:
        return 0.0, 0.0, 0.0

    tr_list, plus_dm_list, minus_dm_list = [], [], []

    for i in range(1, len(ohlcv)):
        high, low = ohlcv[i]["high"], ohlcv[i]["low"]
        prev_high = ohlcv[i - 1]["high"]
        prev_low = ohlcv[i - 1]["low"]
        prev_close = ohlcv[i - 1]["close"]

        # True Range
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        tr_list.append(tr)

        # Directional Movement
        up_move = high - prev_high
        down_move = prev_low - low

        if up_move > down_move and up_move > 0:
            plus_dm_list.append(up_move)
        else:
            plus_dm_list.append(0.0)

        if down_move > up_move and down_move > 0:
            minus_dm_list.append(down_move)
        else:
            minus_dm_list.append(0.0)

    # Wilder's smoothing (EMA with alpha=1/period)
    atr_smooth = sum(tr_list[:period]) / period
    plus_dm_smooth = sum(plus_dm_list[:period]) / period
    minus_dm_smooth = sum(minus_dm_list[:period]) / period

    dx_values = []
    for i in range(period, len(tr_list)):
        atr_smooth = (atr_smooth * (period - 1) + tr_list[i]) / period
        plus_dm_smooth = (plus_dm_smooth * (period - 1) + plus_dm_list[i]) / period
        minus_dm_smooth = (minus_dm_smooth * (period - 1) + minus_dm_list[i]) / period

        if atr_smooth > 0:
            pdi = (plus_dm_smooth / atr_smooth) * 100
            mdi = (minus_dm_smooth / atr_smooth) * 100
            dx = abs(pdi - mdi) / (pdi + mdi) * 100 if (pdi + mdi) > 0 else 0.0
            dx_values.append(dx)

    # ADX = Wilder's smoothed DX
    if len(dx_values) >= period:
        adx_val = sum(dx_values[:period]) / period
        for i in range(period, len(dx_values)):
            adx_val = (adx_val * (period - 1) + dx_values[i]) / period
    elif dx_values:
        adx_val = sum(dx_values) / len(dx_values)
    else:
        adx_val = 0.0

    # Final +DI/-DI
    final_pdi = (plus_dm_smooth / atr_smooth * 100) if atr_smooth > 0 else 0.0
    final_mdi = (minus_dm_smooth / atr_smooth * 100) if atr_smooth > 0 else 0.0

    return adx_val, final_pdi, final_mdi


# ---------------------------------------------------------------------------
# ATR (Average True Range)
# ---------------------------------------------------------------------------

def atr(ohlcv: List[Dict[str, float]], period: int = 14) -> List[float]:
    """
    Compute ATR series using Wilder's smoothing.

    Returns:
        List of ATR values (same length as ohlcv, first values use SMA seeding).
    """
    if len(ohlcv) < 2:
        return [0.0] * len(ohlcv)

    tr_values = [0.0]
    for i in range(1, len(ohlcv)):
        h, l = ohlcv[i]["high"], ohlcv[i]["low"]
        pc = ohlcv[i - 1]["close"]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        tr_values.append(tr)

    result = [0.0] * len(ohlcv)
    if len(tr_values) <= period:
        avg = sum(tr_values) / len(tr_values)
        return [avg] * len(ohlcv)

    # Seed with SMA
    result[period] = sum(tr_values[1:period + 1]) / period
    for i in range(period + 1, len(tr_values)):
        result[i] = (result[i - 1] * (period - 1) + tr_values[i]) / period

    for i in range(period):
        result[i] = result[period]

    return result


def latest_atr(ohlcv: List[Dict[str, float]], period: int = 14) -> float:
    """Get the most recent ATR value."""
    vals = atr(ohlcv, period)
    return vals[-1] if vals else 0.0


# ---------------------------------------------------------------------------
# RSI (Relative Strength Index)
# ---------------------------------------------------------------------------

def rsi(closes: List[float], period: int = 14) -> List[float]:
    """
    Compute RSI using Wilder's smoothing.

    Returns:
        List of RSI values [0, 100].
    """
    if len(closes) < period + 1:
        return [50.0] * len(closes)

    gains, losses = [], []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))

    result = [50.0] * len(closes)

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        if avg_loss == 0:
            result[i + 1] = 100.0
        else:
            rs = avg_gain / avg_loss
            result[i + 1] = 100.0 - (100.0 / (1.0 + rs))

        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    return result


# ---------------------------------------------------------------------------
# Bollinger Bands
# ---------------------------------------------------------------------------

def bollinger_bands(
    closes: List[float],
    period: int = 20,
    num_std: float = 2.0,
) -> Tuple[List[float], List[float], List[float]]:
    """
    Compute Bollinger Bands.

    Returns:
        Tuple of (middle_band, upper_band, lower_band).
    """
    if len(closes) < period:
        empty = [0.0] * len(closes)
        return empty, empty, empty

    middle = sma(closes, period)
    upper, lower = [0.0] * len(closes), [0.0] * len(closes)

    for i in range(period - 1, len(closes)):
        window = closes[i - period + 1:i + 1]
        mean = sum(window) / period
        variance = sum((x - mean) ** 2 for x in window) / period
        std = variance ** 0.5
        upper[i] = middle[i] + num_std * std
        lower[i] = middle[i] - num_std * std

    return middle, upper, lower


# ---------------------------------------------------------------------------
# Volatility Estimators
# ---------------------------------------------------------------------------

def realized_volatility(closes: List[float], window: int = 20, annualize: bool = True) -> float:
    """
    Realized volatility (standard deviation of log returns).

    Args:
        closes: Close price series.
        window: Lookback window.
        annualize: If True, scale to annualized vol (sqrt(365 * 24 * 60 / interval_minutes)).

    Returns:
        Volatility as decimal (e.g., 0.02 = 2%).
    """
    if len(closes) < window + 1:
        return 0.0

    recent = closes[-window - 1:]
    log_returns = []
    for i in range(1, len(recent)):
        if recent[i - 1] > 0 and recent[i] > 0:
            log_returns.append(math.log(recent[i] / recent[i - 1]))

    if not log_returns:
        return 0.0

    mean = sum(log_returns) / len(log_returns)
    variance = sum((r - mean) ** 2 for r in log_returns) / (len(log_returns) - 1)
    vol = variance ** 0.5

    if annualize:
        # Assume 5-min candles: 12 * 24 * 365 = 105120 periods/year
        vol *= (105120) ** 0.5

    return vol


def parkinson_volatility(ohlcv: List[Dict[str, float]], window: int = 20) -> float:
    """
    Parkinson volatility estimator (uses high-low range).

    More efficient than close-to-close: uses ~5× less data for same accuracy.
    σ² = (1 / (4 * N * ln(2))) * Σ (ln(high/low))²
    """
    if len(ohlcv) < window:
        return 0.0

    recent = ohlcv[-window:]
    sum_sq = 0.0
    for bar in recent:
        if bar["high"] > 0 and bar["low"] > 0:
            sum_sq += (math.log(bar["high"] / bar["low"])) ** 2

    n = len(recent)
    var = sum_sq / (4 * n * math.log(2))
    return var ** 0.5


# ---------------------------------------------------------------------------
# Volume Indicators
# ---------------------------------------------------------------------------

def vwap(ohlcv: List[Dict[str, float]]) -> float:
    """
    Volume-Weighted Average Price for the most recent period.

    VWAP = Σ(typical_price * volume) / Σ(volume)
    """
    if not ohlcv:
        return 0.0

    total_pv = 0.0
    total_vol = 0.0
    for bar in ohlcv:
        typical = (bar["high"] + bar["low"] + bar["close"]) / 3.0
        vol = bar.get("volume", 0)
        total_pv += typical * vol
        total_vol += vol

    return total_pv / total_vol if total_vol > 0 else 0.0


def relative_volume(ohlcv: List[Dict[str, float]], window: int = 20) -> float:
    """
    Current volume relative to N-period average.

    Returns:
        Ratio > 1 = above average, < 1 = below average.
    """
    if len(ohlcv) < 2:
        return 1.0

    current_vol = ohlcv[-1].get("volume", 0)
    historical = ohlcv[-window - 1:-1] if len(ohlcv) > window else ohlcv[:-1]
    avg_vol = sum(b.get("volume", 0) for b in historical) / max(1, len(historical))

    return current_vol / avg_vol if avg_vol > 0 else 1.0


# ---------------------------------------------------------------------------
# Composite Indicator Bundle
# ---------------------------------------------------------------------------

@dataclass
class IndicatorBundle:
    """All indicators for a single symbol at a single point in time."""
    symbol: str
    timestamp: str = ""

    # Price
    close: float = 0.0
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0

    # Moving averages
    ema_fast: float = 0.0
    ema_slow: float = 0.0

    # MACD
    macd_line: float = 0.0
    macd_signal: float = 0.0
    macd_hist: float = 0.0

    # ADX
    adx: float = 0.0
    plus_di: float = 0.0
    minus_di: float = 0.0

    # ATR & Volatility
    atr: float = 0.0
    volatility: float = 0.0

    # RSI
    rsi: float = 50.0

    # Bollinger Bands
    bb_upper: float = 0.0
    bb_middle: float = 0.0
    bb_lower: float = 0.0

    # Volume
    volume: float = 0.0
    relative_volume: float = 1.0
    vwap: float = 0.0

    def to_dict(self) -> Dict[str, float]:
        return {
            "close": self.close,
            "ema_fast": self.ema_fast,
            "ema_slow": self.ema_slow,
            "macd_hist": self.macd_hist,
            "adx": self.adx,
            "plus_di": self.plus_di,
            "minus_di": self.minus_di,
            "atr": self.atr,
            "volatility": self.volatility,
            "rsi": self.rsi,
            "bb_upper": self.bb_upper,
            "bb_lower": self.bb_lower,
            "relative_volume": self.relative_volume,
            "vwap": self.vwap,
        }


def compute_all(
    symbol: str,
    ohlcv: List[Dict[str, float]],
    ema_fast_period: int = 12,
    ema_slow_period: int = 26,
    macd_signal_period: int = 9,
    adx_period: int = 14,
    atr_period: int = 14,
    rsi_period: int = 14,
    bb_period: int = 20,
    vol_window: int = 20,
) -> IndicatorBundle:
    """
    Compute ALL indicators for a symbol from OHLCV data.

    This is the main entry point for the data layer. Call once per tick
    per symbol.

    Args:
        symbol: Trading pair.
        ohlcv: Full OHLCV history (most recent last).
        * Various indicator periods.

    Returns:
        An IndicatorBundle with all computed values.
    """
    if not ohlcv:
        return IndicatorBundle(symbol=symbol)

    closes = [c["close"] for c in ohlcv]
    latest = ohlcv[-1]

    # EMA
    ema_fast_series = ema(closes, ema_fast_period)
    ema_slow_series = ema(closes, ema_slow_period)

    # MACD
    macd_line, signal_line, histogram = macd(closes, ema_fast_period, ema_slow_period, macd_signal_period)

    # ADX
    adx_val, pdi, mdi = adx(ohlcv, adx_period)

    # ATR
    atr_series = atr(ohlcv, atr_period)

    # RSI
    rsi_series = rsi(closes, rsi_period)

    # Bollinger
    bb_mid, bb_up, bb_low = bollinger_bands(closes, bb_period)

    # Volume
    rel_vol = relative_volume(ohlcv, vol_window)
    vwap_val = vwap(ohlcv[-vol_window:] if len(ohlcv) > vol_window else ohlcv)

    # Volatility
    realized_vol = realized_volatility(closes, vol_window, annualize=False)

    return IndicatorBundle(
        symbol=symbol,
        timestamp=latest.get("timestamp", ""),
        close=latest["close"],
        open=latest["open"],
        high=latest["high"],
        low=latest["low"],
        ema_fast=ema_fast_series[-1] if ema_fast_series else 0.0,
        ema_slow=ema_slow_series[-1] if ema_slow_series else 0.0,
        macd_line=macd_line[-1] if macd_line else 0.0,
        macd_signal=signal_line[-1] if signal_line else 0.0,
        macd_hist=histogram[-1] if histogram else 0.0,
        adx=adx_val,
        plus_di=pdi,
        minus_di=mdi,
        atr=atr_series[-1] if atr_series else 0.0,
        volatility=realized_vol,
        rsi=rsi_series[-1] if rsi_series else 50.0,
        bb_upper=bb_up[-1] if bb_up else 0.0,
        bb_middle=bb_mid[-1] if bb_mid else 0.0,
        bb_lower=bb_low[-1] if bb_low else 0.0,
        volume=latest.get("volume", 0.0),
        relative_volume=rel_vol,
        vwap=vwap_val,
    )
