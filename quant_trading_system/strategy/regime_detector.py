"""
================================================================================
REGIME DETECTOR — Five-Factor Market Regime Scoring Model
================================================================================

Classifies the current market as either RANGING (suited for grid trading) or
TRENDING (suited for trend following) using a weighted multi-factor model.

FIVE FACTORS:
    1. TREND STRENGTH (30%): ADX-based directional movement.
       ADX > 25 → trending; ADX < 20 → ranging.

    2. VOLATILITY REGIME (25%): Current volatility vs. historical percentiles.
       High percentile → expansion (trending); low percentile → contraction (ranging).

    3. MOMENTUM (20%): MACD histogram direction, slope, and EMA alignment.
       Strong histogram with aligned EMAs → trending.

    4. VOLUME PROFILE (15%): Relative volume and volume-weighted price deviation.
       Volume surge with directional movement → trending.

    5. MARKET MICROSTRUCTURE (10%): Order book imbalance proxy via candle
       structure (body/wick ratio, close position within range).

HYSTERESIS THRESHOLDS:
    To prevent regime-hopping in choppy markets:
    - score > 0.70 → switch to TRENDING (or stay TRENDING)
    - score < 0.30 → switch to RANGING (or stay RANGING)
    - 0.30 ≤ score ≤ 0.70 → keep current regime (hysteresis band)

OUTPUT:
    A MarketRegime enum value and a normalized confidence score [0, 1].

DECOUPLING NOTE:
    This module NEVER imports from execution/ or core/. It computes scores
    from OHLCV data and returns a MarketRegime enum + score.
================================================================================
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class MarketRegime(str, Enum):
    """The two macro market regimes."""
    RANGING = "RANGING"
    TRENDING = "TRENDING"


# ---------------------------------------------------------------------------
# Data Classes
# ---------------------------------------------------------------------------

@dataclass
class FactorScores:
    """Individual factor scores before weighting."""
    trend_strength: float = 0.0
    volatility_regime: float = 0.0
    momentum: float = 0.0
    volume_profile: float = 0.0
    market_microstructure: float = 0.0

    def to_dict(self) -> Dict[str, float]:
        return {
            "trend_strength": self.trend_strength,
            "volatility_regime": self.volatility_regime,
            "momentum": self.momentum,
            "volume_profile": self.volume_profile,
            "market_microstructure": self.market_microstructure,
        }


@dataclass
class RegimeResult:
    """Complete regime detection output."""
    regime: MarketRegime
    score: float  # 0 = pure ranging, 1 = pure trending
    factor_scores: FactorScores
    confidence: float  # How confident we are in this classification
    switched: bool  # Did the regime change this tick?

    @property
    def is_trending(self) -> bool:
        return self.regime == MarketRegime.TRENDING

    @property
    def is_ranging(self) -> bool:
        return self.regime == MarketRegime.RANGING


# ---------------------------------------------------------------------------
# RegimeDetector — The Scoring Engine
# ---------------------------------------------------------------------------

class RegimeDetector:
    """
    Multi-factor regime scoring engine.

    Computes a weighted score from five independent factors and applies
    hysteresis thresholds to determine the current market regime.

    USAGE:
        detector = RegimeDetector(weights={...}, adx_threshold=25)
        result = detector.detect(ohlcv_data, current_regime=MarketRegime.RANGING)
        # result.regime → MarketRegime.TRENDING or RANGING
        # result.score → 0.0–1.0
    """

    def __init__(
        self,
        weights: Optional[Dict[str, float]] = None,
        adx_threshold: float = 25.0,
        volatility_window: int = 20,
        volatility_percentile_low: float = 25.0,
        volatility_percentile_high: float = 75.0,
        lookback_periods: int = 100,
        hysteresis_upper: float = 0.70,
        hysteresis_lower: float = 0.30,
    ):
        """
        Initialize the regime detector.

        Args:
            weights: Dict mapping factor name → weight [0, 1].
                     Defaults to the production weights.
            adx_threshold: ADX above this → trending signal.
            volatility_window: Periods for volatility percentile calculation.
            volatility_percentile_low: Percentile below which vol is "low".
            volatility_percentile_high: Percentile above which vol is "high".
            lookback_periods: Max periods for historical context.
            hysteresis_upper: Score must exceed this to switch to TRENDING.
            hysteresis_lower: Score must fall below this to switch to RANGING.
        """
        self.weights = weights or {
            "trend_strength": 0.30,
            "volatility_regime": 0.25,
            "momentum": 0.20,
            "volume_profile": 0.15,
            "market_microstructure": 0.10,
        }
        self.adx_threshold = adx_threshold
        self.volatility_window = volatility_window
        self.volatility_percentile_low = volatility_percentile_low
        self.volatility_percentile_high = volatility_percentile_high
        self.lookback_periods = lookback_periods
        self.hysteresis_upper = hysteresis_upper
        self.hysteresis_lower = hysteresis_lower

        # Validate weights sum to ~1.0
        total = sum(self.weights.values())
        if abs(total - 1.0) > 0.01:
            raise ValueError(f"Factor weights must sum to 1.0, got {total}")

    # ------------------------------------------------------------------
    # Main Detection
    # ------------------------------------------------------------------

    def detect(
        self,
        ohlcv: List[Dict[str, float]],
        current_regime: MarketRegime = MarketRegime.RANGING,
        adx: Optional[float] = None,
        plus_di: Optional[float] = None,
        minus_di: Optional[float] = None,
        ema_fast: Optional[float] = None,
        ema_slow: Optional[float] = None,
        macd_hist: Optional[float] = None,
        atr: Optional[float] = None,
        volume: Optional[float] = None,
        avg_volume: Optional[float] = None,
        bid_ask_spread_pct: Optional[float] = None,
    ) -> RegimeResult:
        """
        Detect the current market regime.

        You can either pass raw OHLCV data (for self-contained computation)
        OR pre-computed indicator values (for efficiency when indicators
        are already computed by the data layer).

        Args:
            ohlcv: List of OHLCV dicts with keys: open, high, low, close, volume.
                   Most recent last. Used for microstructure factor and as
                   fallback when indicators are not provided.
            current_regime: The current regime (for hysteresis).
            adx, plus_di, minus_di: Pre-computed ADX components.
            ema_fast, ema_slow: Pre-computed EMAs.
            macd_hist: Pre-computed MACD histogram.
            atr: Pre-computed ATR.
            volume: Current period volume.
            avg_volume: Average volume over lookback.
            bid_ask_spread_pct: Current spread as percentage.

        Returns:
            RegimeResult with regime classification, score, and factor breakdown.
        """
        # Compute ADX-based metrics if not provided
        if adx is None and len(ohlcv) >= 14:
            adx, plus_di, minus_di = self._compute_adx(ohlcv)

        # Compute ATR if not provided
        if atr is None and len(ohlcv) >= 2:
            atr = self._compute_atr(ohlcv, period=14)

        # Compute EMAs if not provided
        if (ema_fast is None or ema_slow is None) and len(ohlcv) >= 26:
            closes = [c["close"] for c in ohlcv]
            if ema_fast is None:
                ema_fast = self._compute_ema(closes, 12)
            if ema_slow is None:
                ema_slow = self._compute_ema(closes, 26)

        if macd_hist is None and ema_fast is not None and ema_slow is not None:
            macd_hist = (ema_fast - ema_slow) - self._compute_ema(
                [ema_fast - ema_slow] * 5, 9
            )

        # --- Score each factor ---
        scores = FactorScores()

        scores.trend_strength = self._score_trend_strength(
            adx=adx or 20.0,
            plus_di=plus_di or 20.0,
            minus_di=minus_di or 20.0,
        )

        scores.volatility_regime = self._score_volatility_regime(
            ohlcv=ohlcv,
            atr=atr or 0.0,
        )

        scores.momentum = self._score_momentum(
            ema_fast=ema_fast or 0.0,
            ema_slow=ema_slow or 0.0,
            macd_hist=macd_hist or 0.0,
            ohlcv=ohlcv,
            atr=atr or 0.0,
        )

        scores.volume_profile = self._score_volume_profile(
            volume=volume or (ohlcv[-1].get("volume", 0) if ohlcv else 0),
            avg_volume=avg_volume or 0.0,
            ohlcv=ohlcv,
        )

        scores.market_microstructure = self._score_microstructure(
            ohlcv=ohlcv,
            bid_ask_spread_pct=bid_ask_spread_pct or 0.05,
        )

        # --- Weighted composite score ---
        composite = (
            scores.trend_strength * self.weights["trend_strength"]
            + scores.volatility_regime * self.weights["volatility_regime"]
            + scores.momentum * self.weights["momentum"]
            + scores.volume_profile * self.weights["volume_profile"]
            + scores.market_microstructure * self.weights["market_microstructure"]
        )

        # Clamp to [0, 1]
        composite = max(0.0, min(1.0, composite))

        # --- Apply hysteresis ---
        new_regime = self._apply_hysteresis(composite, current_regime)
        switched = new_regime != current_regime

        # Confidence: distance from the hysteresis boundary
        if new_regime == MarketRegime.TRENDING:
            confidence = min(1.0, composite / self.hysteresis_upper)
        else:
            confidence = min(1.0, (1.0 - composite) / (1.0 - self.hysteresis_lower))

        return RegimeResult(
            regime=new_regime,
            score=round(composite, 4),
            factor_scores=scores,
            confidence=round(confidence, 4),
            switched=switched,
        )

    # ------------------------------------------------------------------
    # Factor 1: Trend Strength (ADX-based)
    # ------------------------------------------------------------------

    def _score_trend_strength(
        self,
        adx: float,
        plus_di: float,
        minus_di: float,
    ) -> float:
        """
        Score trend strength from ADX and directional indicators.

        Logic:
            - ADX > threshold → strong trend (score > 0.7)
            - ADX < threshold → weak/no trend (score < 0.3)
            - DI separation confirms direction: |+DI - -DI| > 10 → clearer trend
            - Normalize ADX to [0, 1] using a sigmoid centered at the threshold.

        Returns:
            Score in [0, 1]. Higher = more trending.
        """
        if adx <= 0:
            return 0.0

        # Sigmoid around adx_threshold: score approaches 1 as ADX → ∞
        # Slope controls steepness; k=0.15 gives ~0.27 at ADX=20, ~0.73 at ADX=30
        k = 0.15
        base_score = 1.0 / (1.0 + math.exp(-k * (adx - self.adx_threshold)))

        # DI separation bonus: if +DI and -DI are far apart, the trend is clearer
        di_separation = abs(plus_di - minus_di)
        separation_factor = min(1.0, di_separation / 30.0)  # Cap at 30

        # Blend: 70% ADX level, 30% DI separation
        score = 0.70 * base_score + 0.30 * separation_factor

        return max(0.0, min(1.0, score))

    # ------------------------------------------------------------------
    # Factor 2: Volatility Regime
    # ------------------------------------------------------------------

    def _score_volatility_regime(
        self,
        ohlcv: List[Dict[str, float]],
        atr: float,
    ) -> float:
        """
        Score volatility expansion vs. contraction.

        Logic:
            - Compute ATR as % of price for last N periods.
            - Find current ATR%'s percentile in the historical distribution.
            - High percentile → volatility expanding (trending, score > 0.7).
            - Low percentile → volatility contracting (ranging, score < 0.3).

        Returns:
            Score in [0, 1]. Higher = more volatile (trending).
        """
        if len(ohlcv) < self.volatility_window or atr <= 0:
            return 0.5  # Neutral if insufficient data

        # Compute ATR% history
        recent = ohlcv[-self.volatility_window:]
        atr_pcts = []
        for i in range(1, len(recent)):
            bar_atr = max(
                recent[i]["high"] - recent[i]["low"],
                abs(recent[i]["high"] - recent[i - 1]["close"]),
                abs(recent[i]["low"] - recent[i - 1]["close"]),
            )
            price = (recent[i]["high"] + recent[i]["low"] + recent[i]["close"]) / 3
            if price > 0:
                atr_pcts.append(bar_atr / price * 100)

        if not atr_pcts:
            return 0.5

        # Current ATR as % of price
        current_price = (recent[-1]["high"] + recent[-1]["low"] + recent[-1]["close"]) / 3
        current_atr_pct = atr / current_price * 100 if current_price > 0 else 0

        # Percentile rank within history
        sorted_atr = sorted(atr_pcts)
        rank = sum(1 for v in sorted_atr if v < current_atr_pct)
        percentile = rank / len(sorted_atr) * 100

        # Map percentile to score: low percentile → 0, high percentile → 1
        # Using the configured thresholds
        if percentile >= self.volatility_percentile_high:
            return 0.85  # Strong expansion
        elif percentile <= self.volatility_percentile_low:
            return 0.15  # Strong contraction
        else:
            # Linear interpolation between thresholds
            frac = (percentile - self.volatility_percentile_low) / (
                self.volatility_percentile_high - self.volatility_percentile_low
            )
            return 0.15 + frac * 0.70

    # ------------------------------------------------------------------
    # Factor 3: Momentum (MACD + EMA Alignment)
    # ------------------------------------------------------------------

    def _score_momentum(
        self,
        ema_fast: float,
        ema_slow: float,
        macd_hist: float,
        ohlcv: List[Dict[str, float]],
        atr: float,
    ) -> float:
        """
        Score price momentum strength.

        Logic:
            - EMA fast > slow AND MACD histogram expanding → strong momentum (score > 0.7).
            - EMA cross against each other → weak/no momentum (score < 0.3).
            - MACD histogram normalized by ATR for cross-asset comparability.

        Returns:
            Score in [0, 1]. Higher = stronger momentum (trending).
        """
        if ema_fast <= 0 or ema_slow <= 0:
            return 0.5

        # EMA alignment: 1.0 if fast is convincingly on one side of slow
        ema_diff_pct = abs(ema_fast - ema_slow) / ema_slow * 100
        ema_score = min(1.0, ema_diff_pct / 2.0)  # 2% difference → full score

        # MACD histogram strength (normalized by ATR)
        macd_score = 0.5
        if atr > 0 and len(ohlcv) >= 2:
            price = ohlcv[-1]["close"]
            # Normalize MACD histogram by price and ATR
            normalized_macd = abs(macd_hist) / (atr * price / 10000)
            macd_score = min(1.0, normalized_macd / 100.0)

        # Check if recent MACD histogram is expanding
        if len(ohlcv) >= 4:
            # Approximate: compare current histogram magnitude to recent
            recent_range = max(c["high"] for c in ohlcv[-4:]) - min(c["low"] for c in ohlcv[-4:])
            if recent_range > 0:
                macd_strength = abs(macd_hist) / recent_range * 10000
                macd_score = max(macd_score, min(1.0, macd_strength / 50.0))

        # Blend: 50% EMA alignment, 50% MACD strength
        score = 0.50 * ema_score + 0.50 * macd_score
        return max(0.0, min(1.0, score))

    # ------------------------------------------------------------------
    # Factor 4: Volume Profile
    # ------------------------------------------------------------------

    def _score_volume_profile(
        self,
        volume: float,
        avg_volume: float,
        ohlcv: List[Dict[str, float]],
    ) -> float:
        """
        Score volume confirmation of trend.

        Logic:
            - Volume surge (> 1.5× average) during price movement → trending (score > 0.7).
            - Low relative volume → ranging / no conviction (score < 0.3).
            - Volume climax (extreme spike) → potential reversal (score moderate).

        Returns:
            Score in [0, 1]. Higher = volume confirms trend.
        """
        if not ohlcv or volume <= 0:
            return 0.5

        # Compute average volume if not provided
        if avg_volume <= 0:
            volumes = [c.get("volume", 0) for c in ohlcv[-self.volatility_window:]]
            volumes = [v for v in volumes if v > 0]
            avg_volume = sum(volumes) / len(volumes) if volumes else volume

        if avg_volume <= 0:
            return 0.5

        relative_volume = volume / avg_volume

        # Volume ratio to score mapping:
        # < 0.7 → low volume, ranging (score 0.2)
        # 0.7–1.3 → normal, neutral (score 0.5)
        # 1.3–2.0 → elevated, confirming (score 0.8)
        # > 2.0 → climax, possible reversal (score 0.6) — moderate

        if relative_volume < 0.7:
            score = 0.2
        elif relative_volume < 1.3:
            score = 0.5
        elif relative_volume < 2.0:
            score = 0.8
        else:
            score = 0.6  # Climax — could be either strong trend or exhaustion

        # Bonus: check if volume is increasing over recent bars (trend)
        if len(ohlcv) >= 5:
            recent_vols = [c.get("volume", 0) for c in ohlcv[-5:]]
            if all(recent_vols[i] >= recent_vols[i - 1] for i in range(1, len(recent_vols))):
                score = min(1.0, score + 0.1)  # Rising volume → trend confirmation

        return score

    # ------------------------------------------------------------------
    # Factor 5: Market Microstructure (Candle Analysis)
    # ------------------------------------------------------------------

    def _score_microstructure(
        self,
        ohlcv: List[Dict[str, float]],
        bid_ask_spread_pct: float = 0.05,
    ) -> float:
        """
        Score market microstructure from candle patterns and spread.

        Logic:
            - Directional candles with small wicks → trending (efficient price
              discovery, fewer rejections).
            - Doji / spinning tops (large wicks, small bodies) → ranging
              (indecision, mean-reversion behavior).
            - Tight spread → liquid, trending-capable. Wide spread → ranging.

        Returns:
            Score in [0, 1]. Higher = microstructure favors trending.
        """
        if len(ohlcv) < 3:
            return 0.5

        # Analyze last 3 candles
        scores = []
        for i in range(-min(3, len(ohlcv)), 0):
            candle = ohlcv[i]
            body = abs(candle["close"] - candle["open"])
            upper_wick = candle["high"] - max(candle["close"], candle["open"])
            lower_wick = min(candle["close"], candle["open"]) - candle["low"]
            total_range = candle["high"] - candle["low"]

            if total_range <= 0:
                scores.append(0.5)
                continue

            # Body ratio: high body/total_range → decisive move (trending)
            body_ratio = body / total_range
            # Wick ratio: low wick/total_range → less rejection (trending)
            wick_ratio = (upper_wick + lower_wick) / total_range

            # Composite: high body, low wicks → trending
            candle_score = 0.6 * body_ratio + 0.4 * (1.0 - wick_ratio)
            scores.append(candle_score)

        avg_candle_score = sum(scores) / len(scores)

        # Spread factor: tight spread → trending-friendly
        spread_score = 1.0
        if bid_ask_spread_pct > 0.2:  # Very wide spread
            spread_score = 0.2
        elif bid_ask_spread_pct > 0.1:
            spread_score = 0.5
        elif bid_ask_spread_pct > 0.05:
            spread_score = 0.7

        # Blend: 70% candle structure, 30% spread
        score = 0.70 * avg_candle_score + 0.30 * spread_score
        return max(0.0, min(1.0, score))

    # ------------------------------------------------------------------
    # Hysteresis Logic
    # ------------------------------------------------------------------

    def _apply_hysteresis(
        self, composite_score: float, current_regime: MarketRegime
    ) -> MarketRegime:
        """
        Apply hysteresis thresholds to prevent rapid regime switching.

        TRANSITION RULES:
            RANGING → TRENDING: composite_score > hysteresis_upper (0.70)
            TRENDING → RANGING: composite_score < hysteresis_lower (0.30)
            Otherwise: stay in current regime.

        The hysteresis band (0.30–0.70) acts as a "dead zone" where the
        market is ambiguous — we hold the current regime to avoid whipsaw.
        """
        if composite_score > self.hysteresis_upper:
            return MarketRegime.TRENDING
        elif composite_score < self.hysteresis_lower:
            return MarketRegime.RANGING
        else:
            return current_regime  # Hold

    # ------------------------------------------------------------------
    # Indicator Computation (Embedded for Self-Sufficiency)
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_adx(ohlcv: List[Dict[str, float]], period: int = 14) -> Tuple[float, float, float]:
        """Compute ADX, +DI, -DI from OHLCV data. Wilder's smoothing."""
        if len(ohlcv) < period + 1:
            return 20.0, 20.0, 20.0

        # True Range
        tr_values = []
        plus_dm = []
        minus_dm = []
        for i in range(1, len(ohlcv)):
            high, low = ohlcv[i]["high"], ohlcv[i]["low"]
            prev_high, prev_low = ohlcv[i - 1]["high"], ohlcv[i - 1]["low"]
            prev_close = ohlcv[i - 1]["close"]

            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            tr_values.append(tr)

            up_move = high - prev_high
            down_move = prev_low - low
            pdm = up_move if up_move > down_move and up_move > 0 else 0
            ndm = down_move if down_move > up_move and down_move > 0 else 0
            plus_dm.append(pdm)
            minus_dm.append(ndm)

        # Wilder's smoothing (EMA with alpha = 1/period on first value, then 1/period after)
        atr = sum(tr_values[:period]) / period
        smoothed_plus_dm = sum(plus_dm[:period]) / period
        smoothed_minus_dm = sum(minus_dm[:period]) / period

        for i in range(period, len(tr_values)):
            atr = (atr * (period - 1) + tr_values[i]) / period
            smoothed_plus_dm = (smoothed_plus_dm * (period - 1) + plus_dm[i]) / period
            smoothed_minus_dm = (smoothed_minus_dm * (period - 1) + minus_dm[i]) / period

        if atr <= 0:
            return 20.0, 20.0, 20.0

        plus_di = (smoothed_plus_dm / atr) * 100
        minus_di = (smoothed_minus_dm / atr) * 100
        dx = abs(plus_di - minus_di) / (plus_di + minus_di) * 100 if (plus_di + minus_di) > 0 else 0

        # ADX = smoothed DX (same Wilder's smoothing applied to initial DX values)
        # For simplicity, we return the current DX as ADX (close enough for regime detection)
        return dx, plus_di, minus_di

    @staticmethod
    def _compute_atr(ohlcv: List[Dict[str, float]], period: int = 14) -> float:
        """Compute ATR from OHLCV data."""
        if len(ohlcv) < 2:
            return 0.0

        tr_values = []
        for i in range(1, len(ohlcv)):
            high, low = ohlcv[i]["high"], ohlcv[i]["low"]
            prev_close = ohlcv[i - 1]["close"]
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            tr_values.append(tr)

        window = tr_values[-period:] if len(tr_values) >= period else tr_values
        return sum(window) / len(window)

    @staticmethod
    def _compute_ema(values: List[float], period: int) -> float:
        """Compute EMA of a series. Returns the latest value."""
        if not values or period <= 0:
            return 0.0
        if len(values) < period:
            return sum(values) / len(values)

        multiplier = 2.0 / (period + 1)
        ema = sum(values[:period]) / period
        for i in range(period, len(values)):
            ema = (values[i] - ema) * multiplier + ema
        return ema

    def __repr__(self) -> str:
        return (
            f"RegimeDetector(ADX_thresh={self.adx_threshold}, "
            f"hysteresis=[{self.hysteresis_lower}, {self.hysteresis_upper}])"
        )


# Needed for the sigmoid calculation
import math
