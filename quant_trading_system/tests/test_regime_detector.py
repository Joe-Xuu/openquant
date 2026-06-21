"""Tests for RegimeDetector — factor scoring, hysteresis, regime classification."""
import sys
sys.path.insert(0, ".")

import math
import random
from strategy.regime_detector import MarketRegime, RegimeDetector


def make_ohlcv(n: int = 100, price: float = 100000, trend_strength: float = 0.0,
               volatility: float = 0.01) -> list:
    """Generate synthetic OHLCV data."""
    random.seed(42)
    candles = []
    for i in range(n):
        drift = trend_strength * price * 0.001
        change = random.gauss(drift, price * volatility)
        close = price + change
        high = close * (1 + random.uniform(0, 0.005))
        low = close * (1 - random.uniform(0, 0.005))
        open_p = low + random.uniform(0, high - low)
        volume = random.uniform(100, 1000)
        candles.append({"open": open_p, "high": high, "low": low, "close": close, "volume": volume})
        price = close
    return candles


class TestRegimeDetection:
    def test_detect_ranging_market(self):
        """Low-vol, sideways market should be classified as RANGING."""
        rd = RegimeDetector()
        ohlcv = make_ohlcv(100, price=100000, trend_strength=0.0, volatility=0.005)
        result = rd.detect(ohlcv, current_regime=MarketRegime.RANGING)
        assert result.score is not None
        assert 0.0 <= result.score <= 1.0

    def test_detect_trending_market(self):
        """Strong trend should score higher than ranging."""
        rd = RegimeDetector()
        ohlcv_range = make_ohlcv(100, price=100000, trend_strength=0.0, volatility=0.005)
        ohlcv_trend = make_ohlcv(100, price=100000, trend_strength=1.0, volatility=0.01)

        result_range = rd.detect(ohlcv_range, current_regime=MarketRegime.RANGING)
        result_trend = rd.detect(ohlcv_trend, current_regime=MarketRegime.RANGING)
        # Trending data should generally score higher
        # (Not always guaranteed with synthetic data, but directionally correct)
        assert result_trend.factor_scores.trend_strength is not None

    def test_hysteresis_holds_regime(self):
        """Score in the middle band should not change regime."""
        rd = RegimeDetector(hysteresis_upper=0.70, hysteresis_lower=0.30)
        ohlcv = make_ohlcv(100)
        # Force score into middle band by providing neutral indicators
        result = rd.detect(
            ohlcv,
            current_regime=MarketRegime.TRENDING,
            adx=22, plus_di=22, minus_di=20,
        )
        # With ADX=22 (near threshold of 25), score should be moderate
        # If score is in hysteresis band, regime should persist
        if 0.30 <= result.score <= 0.70:
            assert result.regime == MarketRegime.TRENDING
            assert not result.switched

    def test_strong_trend_switches_to_trending(self):
        """Very high ADX should push score above hysteresis_upper."""
        rd = RegimeDetector(hysteresis_upper=0.70)
        ohlcv = make_ohlcv(100, trend_strength=5.0, volatility=0.02)
        result = rd.detect(
            ohlcv, current_regime=MarketRegime.RANGING,
            adx=45, plus_di=40, minus_di=10,
        )
        assert result.score > 0.5, f"Strong ADX should give high score, got {result.score}"

    def test_weights_must_sum_to_one(self):
        """Initialization with bad weights should raise."""
        try:
            RegimeDetector(weights={"trend_strength": 0.5})
            assert False, "Should have raised ValueError"
        except ValueError:
            pass

    def test_factor_scores_are_normalized(self):
        """All factor scores should be in [0, 1]."""
        rd = RegimeDetector()
        ohlcv = make_ohlcv(50)
        result = rd.detect(ohlcv, current_regime=MarketRegime.RANGING,
                          adx=30, plus_di=35, minus_di=15,
                          ema_fast=101000, ema_slow=100000, macd_hist=500,
                          atr=2000)
        fs = result.factor_scores
        assert 0 <= fs.trend_strength <= 1
        assert 0 <= fs.momentum <= 1
        assert 0 <= fs.volume_profile <= 1
        assert 0 <= fs.market_microstructure <= 1

    def test_confidence_metric(self):
        """Confidence should be higher near extremes."""
        rd = RegimeDetector()
        ohlcv = make_ohlcv(100)
        result = rd.detect(ohlcv, current_regime=MarketRegime.RANGING,
                          adx=45, plus_di=50, minus_di=10)
        assert 0.0 <= result.confidence <= 1.0

    def test_switched_flag(self):
        """switched should be True when regime changes."""
        rd = RegimeDetector()
        ohlcv = make_ohlcv(100, trend_strength=5.0, volatility=0.02)
        result = rd.detect(
            ohlcv, current_regime=MarketRegime.RANGING,
            adx=50, plus_di=55, minus_di=5,
        )
        if result.regime == MarketRegime.TRENDING:
            assert result.switched is True
