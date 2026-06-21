"""
================================================================================
TESTS — Grid Strategy & Signal Contract
================================================================================

Covers:
    1. StrategySignal creation, validation, serialization
    2. Geometric grid layout correctness
    3. Arithmetic grid layout correctness
    4. Grid level ordering & bounds compliance
    5. Take-profit price correctness
    6. Rebalance detection triggers
    7. Grid overlap prevention
    8. Signal generation from GridConfig
    9. Dynamic bound optimization
   10. Edge cases: zero levels, negative prices, invalid configs
   11. Decoupling: strategy/ does not import execution/ or core/
================================================================================
"""

import json
import math
import sys
from datetime import datetime, timezone

import pytest

# Ensure the project root is on the path
sys.path.insert(0, ".")

from strategy.signal import SignalAction, SignalBuilder, StrategySignal
from strategy.grid_strategy import (
    GridConfig,
    GridLevel,
    GridStatus,
    GridStrategy,
    GridType,
    LevelSide,
    LevelStatus,
)


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def geometric_grid() -> GridStrategy:
    """Default geometric grid engine: BTC at $100k, ±5%, 10 levels per side."""
    return GridStrategy(
        grid_type="geometric",
        upper_bound_pct=5.0,
        lower_bound_pct=5.0,
        grid_levels=10,
        profit_per_grid_pct=0.5,
        total_capital=10000.0,
    )


@pytest.fixture
def arithmetic_grid() -> GridStrategy:
    """Default arithmetic grid engine."""
    return GridStrategy(
        grid_type="arithmetic",
        upper_bound_pct=5.0,
        lower_bound_pct=5.0,
        grid_levels=10,
        profit_per_grid_pct=0.5,
        total_capital=10000.0,
    )


@pytest.fixture
def btc_grid_config(geometric_grid) -> GridConfig:
    """Precomputed BTC grid at $100k."""
    return geometric_grid.compute_grid(reference_price=100000.0, symbol="BTCUSDT")


@pytest.fixture
def sample_ohlcv() -> list:
    """Generate 50 candles of fake BTC OHLCV data around $100k."""
    import random
    random.seed(42)
    candles = []
    price = 100000.0
    for i in range(50):
        high = price * (1 + random.uniform(0.001, 0.015))
        low = price * (1 - random.uniform(0.001, 0.015))
        close = (high + low) / 2
        candles.append({"high": high, "low": low, "close": close})
        # Random walk with mild uptrend
        price = close * (1 + random.uniform(-0.01, 0.012))
    return candles


# ============================================================================
# TESTS: StrategySignal
# ============================================================================

class TestStrategySignal:
    """Test the signal contract: creation, validation, serialization."""

    def test_create_valid_signal(self):
        """A valid signal should be created without error."""
        signal = StrategySignal(
            action=SignalAction.START_GRID,
            symbol="BTCUSDT",
            score=0.85,
            strategy_id="grid_v1",
        )
        assert signal.action == SignalAction.START_GRID
        assert signal.symbol == "BTCUSDT"
        assert signal.score == 0.85
        assert signal.strategy_id == "grid_v1"
        assert signal.signal_id  # auto-generated
        assert signal.expires_at  # auto-computed

    def test_score_out_of_range_raises(self):
        """Score must be in [0.0, 1.0]."""
        with pytest.raises(ValueError, match="score must be in"):
            StrategySignal(action=SignalAction.NEUTRAL, symbol="BTCUSDT", score=1.5)

        with pytest.raises(ValueError, match="score must be in"):
            StrategySignal(action=SignalAction.NEUTRAL, symbol="BTCUSDT", score=-0.1)

    def test_empty_symbol_raises(self):
        """Symbol must be non-empty."""
        with pytest.raises(ValueError, match="symbol must be"):
            StrategySignal(action=SignalAction.NEUTRAL, symbol="", score=0.5)

    def test_expiry_is_set(self):
        """Signal should expire 30 seconds after creation by default."""
        signal = StrategySignal(
            action=SignalAction.START_TREND, symbol="ETHUSDT", score=0.7
        )
        # Should not be expired immediately
        assert not signal.is_expired()
        assert signal.seconds_until_expiry() > 0

    def test_signal_id_is_deterministic(self):
        """Same params → same signal_id (idempotency)."""
        s1 = StrategySignal(
            action=SignalAction.START_GRID,
            symbol="BTCUSDT",
            score=0.9,
            metadata={"grid_type": "geometric", "levels": 10},
        )
        s2 = StrategySignal(
            action=SignalAction.START_GRID,
            symbol="BTCUSDT",
            score=0.9,
            metadata={"grid_type": "geometric", "levels": 10},
        )
        assert s1.signal_id == s2.signal_id

    def test_signal_id_differs_for_different_params(self):
        """Different params → different signal_id."""
        s1 = StrategySignal(
            action=SignalAction.START_GRID, symbol="BTCUSDT", score=0.9
        )
        s2 = StrategySignal(
            action=SignalAction.START_GRID, symbol="ETHUSDT", score=0.9
        )
        assert s1.signal_id != s2.signal_id

    def test_routing_helpers(self):
        """is_grid_signal() and is_trend_signal() should classify correctly."""
        grid_sig = StrategySignal(action=SignalAction.START_GRID, symbol="BTCUSDT", score=0.8)
        trend_sig = StrategySignal(action=SignalAction.START_TREND, symbol="BTCUSDT", score=0.8)
        neutral_sig = StrategySignal(action=SignalAction.NEUTRAL, symbol="BTCUSDT", score=0.5)

        assert grid_sig.is_grid_signal()
        assert not grid_sig.is_trend_signal()

        assert trend_sig.is_trend_signal()
        assert not trend_sig.is_grid_signal()

        assert not neutral_sig.is_grid_signal()
        assert not neutral_sig.is_trend_signal()
        assert not neutral_sig.requires_order  # NEUTRAL → no order needed

    def test_serialization_roundtrip(self):
        """to_dict() → from_dict() should be lossless."""
        original = StrategySignal(
            action=SignalAction.START_GRID,
            symbol="BTCUSDT",
            score=0.9,
            metadata={"grid_type": "geometric", "levels": 10},
            strategy_id="grid_v1",
        )
        restored = StrategySignal.from_dict(original.to_dict())
        assert restored.action == original.action
        assert restored.symbol == original.symbol
        assert restored.score == original.score
        assert restored.metadata == original.metadata
        assert restored.signal_id == original.signal_id

    def test_json_serialization_roundtrip(self):
        """to_json() → from_json() should be lossless."""
        original = StrategySignal(
            action=SignalAction.START_GRID,
            symbol="BTCUSDT",
            score=0.9,
            metadata={"grid_type": "geometric", "nested": {"key": [1, 2, 3]}},
        )
        json_str = original.to_json()
        restored = StrategySignal.from_json(json_str)
        assert restored.action == original.action
        assert restored.metadata == original.metadata

    def test_is_emergency(self):
        """CLOSE_ALL should be recognized as emergency."""
        emergency = StrategySignal(action=SignalAction.CLOSE_ALL, symbol="BTCUSDT", score=1.0)
        normal = StrategySignal(action=SignalAction.START_GRID, symbol="BTCUSDT", score=0.8)

        assert emergency.is_emergency()
        assert not normal.is_emergency()


# ============================================================================
# TESTS: SignalBuilder
# ============================================================================

class TestSignalBuilder:
    """Test the fluent builder API."""

    def test_builder_creates_valid_signal(self):
        signal = (
            SignalBuilder(SignalAction.START_GRID, "BTCUSDT", 0.85)
            .with_metadata("grid_type", "geometric")
            .with_metadata("levels", 10)
            .with_strategy("grid_v1")
            .with_ttl(60)
            .build()
        )
        assert signal.action == SignalAction.START_GRID
        assert signal.symbol == "BTCUSDT"
        assert signal.score == 0.85
        assert signal.metadata["grid_type"] == "geometric"
        assert signal.metadata["levels"] == 10
        assert signal.strategy_id == "grid_v1"
        # 60s TTL → expiry should be ~60s from now
        assert 55 < signal.seconds_until_expiry() <= 60

    def test_builder_bulk_metadata(self):
        signal = (
            SignalBuilder(SignalAction.START_TREND, "ETHUSDT", 0.7)
            .with_metadata_bulk({"entry": 3500.0, "stop": 3400.0})
            .build()
        )
        assert signal.metadata["entry"] == 3500.0
        assert signal.metadata["stop"] == 3400.0


# ============================================================================
# TESTS: GridStrategy — Geometric Grid
# ============================================================================

class TestGeometricGrid:
    """Test geometric grid layout correctness."""

    def test_correct_number_of_levels(self, btc_grid_config):
        """A grid with N levels should produce 2*N total levels (N buy + N sell)."""
        assert len(btc_grid_config.levels) == 20  # 10 buy + 10 sell
        assert len(btc_grid_config.buy_levels) == 10
        assert len(btc_grid_config.sell_levels) == 10

    def test_buy_levels_below_reference(self, btc_grid_config):
        """All buy levels must be below the reference price."""
        for lvl in btc_grid_config.buy_levels:
            assert lvl.price < 100000.0, f"Buy level {lvl.level_index} at {lvl.price}"

    def test_sell_levels_above_reference(self, btc_grid_config):
        """All sell levels must be above the reference price."""
        for lvl in btc_grid_config.sell_levels:
            assert lvl.price > 100000.0, f"Sell level {lvl.level_index} at {lvl.price}"

    def test_levels_within_bounds(self, btc_grid_config):
        """All levels must be within the configured bounds."""
        lower = 100000.0 * 0.95  # -5%
        upper = 100000.0 * 1.05  # +5%
        for lvl in btc_grid_config.levels:
            assert lower * 0.999 <= lvl.price <= upper * 1.001, (
                f"Level {lvl.level_index} price {lvl.price} outside [{lower}, {upper}]"
            )

    def test_geometric_spacing_is_percentage_based(self, geometric_grid):
        """
        Geometric grid: the RATIO between adjacent levels should be constant.

        For sell levels: price[i+1] / price[i] ≈ constant
        For buy levels: price[i] / price[i+1] ≈ constant
        """
        grid = geometric_grid.compute_grid(reference_price=100000.0, symbol="BTCUSDT")

        sells = grid.sell_levels  # sorted ascending by price
        buy_ratios = []
        for i in range(len(sells) - 1):
            buy_ratios.append(sells[i + 1].price / sells[i].price)

        # All ratios should be nearly equal
        assert max(buy_ratios) - min(buy_ratios) < 0.0001, (
            f"Geometric sell ratios not constant: {[round(r, 6) for r in buy_ratios]}"
        )

    def test_take_profit_buy_levels(self, btc_grid_config):
        """Buy level take-profit must be higher than entry price."""
        for lvl in btc_grid_config.buy_levels:
            assert lvl.take_profit_price > lvl.price, (
                f"Buy TP {lvl.take_profit_price} <= entry {lvl.price}"
            )

    def test_take_profit_sell_levels(self, btc_grid_config):
        """Sell level take-profit must be lower than entry price (for short grid)."""
        for lvl in btc_grid_config.sell_levels:
            assert lvl.take_profit_price < lvl.price, (
                f"Sell TP {lvl.take_profit_price} >= entry {lvl.price}"
            )

    def test_take_profit_matches_profit_pct(self, geometric_grid):
        """Take-profit should be exactly profit_per_grid_pct away from entry."""
        grid = geometric_grid.compute_grid(reference_price=100000.0, symbol="BTCUSDT")
        profit_pct = 0.5  # 0.5%

        for lvl in grid.buy_levels:
            expected_tp = lvl.price * (1 + profit_pct / 100)
            assert abs(lvl.take_profit_price - expected_tp) < lvl.price * 0.001

    def test_no_buy_sell_overlap(self, btc_grid_config):
        """
        CRITICAL: Highest buy price must be below lowest sell price.
        If they overlap, orders would cross-fill instantly → guaranteed loss.
        """
        max_buy = max(lvl.price for lvl in btc_grid_config.buy_levels)
        min_sell = min(lvl.price for lvl in btc_grid_config.sell_levels)
        assert max_buy < min_sell, (
            f"GRID OVERLAP: max_buy={max_buy} >= min_sell={min_sell}"
        )

    def test_levels_are_sorted_by_price(self, btc_grid_config):
        """Buy levels should be sorted descending, sell levels ascending."""
        buys = btc_grid_config.buy_levels
        sells = btc_grid_config.sell_levels

        for i in range(len(buys) - 1):
            assert buys[i].price >= buys[i + 1].price, "Buy levels not sorted descending"

        for i in range(len(sells) - 1):
            assert sells[i].price <= sells[i + 1].price, "Sell levels not sorted ascending"


# ============================================================================
# TESTS: GridStrategy — Arithmetic Grid
# ============================================================================

class TestArithmeticGrid:
    """Test arithmetic grid layout correctness."""

    def test_arithmetic_spacing_is_constant(self, arithmetic_grid):
        """
        Arithmetic grid: the PRICE DIFFERENCE between adjacent levels
        should be constant.
        """
        grid = arithmetic_grid.compute_grid(reference_price=100000.0, symbol="BTCUSDT")

        sells = grid.sell_levels
        gaps = [sells[i + 1].price - sells[i].price for i in range(len(sells) - 1)]

        # All gaps should be nearly equal
        assert max(gaps) - min(gaps) < 0.02, (
            f"Arithmetic sell gaps not constant: {[round(g, 4) for g in gaps]}"
        )

    def test_arithmetic_correct_number_of_levels(self, arithmetic_grid):
        grid = arithmetic_grid.compute_grid(reference_price=50000.0, symbol="ETHUSDT")
        assert len(grid.levels) == 20
        assert len(grid.buy_levels) == 10
        assert len(grid.sell_levels) == 10


# ============================================================================
# TESTS: Rebalancing
# ============================================================================

class TestRebalance:
    """Test grid rebalancing logic."""

    def test_rebalance_when_price_outside_upper_bound(self, geometric_grid):
        """Price above upper_bound → trigger rebalance."""
        grid = geometric_grid.compute_grid(reference_price=100000.0, symbol="BTCUSDT")
        # Upper bound = 100000 * 1.05 = 105000
        assert geometric_grid.check_rebalance(106000.0, grid) is True

    def test_rebalance_when_price_outside_lower_bound(self, geometric_grid):
        """Price below lower_bound → trigger rebalance."""
        grid = geometric_grid.compute_grid(reference_price=100000.0, symbol="BTCUSDT")
        # Lower bound = 100000 * 0.95 = 95000
        assert geometric_grid.check_rebalance(94000.0, grid) is True

    def test_rebalance_when_deviation_exceeds_threshold(self, geometric_grid):
        """Price moved > rebalance_threshold_pct from reference → trigger."""
        grid = geometric_grid.compute_grid(reference_price=100000.0, symbol="BTCUSDT")
        # 1.5% deviation > 1.0% threshold
        assert geometric_grid.check_rebalance(101500.0, grid) is True

    def test_no_rebalance_within_threshold(self, geometric_grid):
        """Price within bounds and threshold → no rebalance."""
        grid = geometric_grid.compute_grid(reference_price=100000.0, symbol="BTCUSDT")
        # 0.5% deviation < 1.0% threshold
        assert geometric_grid.check_rebalance(100450.0, grid) is False

    def test_rebalance_when_all_buy_levels_filled(self, geometric_grid):
        """All buy levels filled → trigger rebalance."""
        grid = geometric_grid.compute_grid(reference_price=100000.0, symbol="BTCUSDT")
        # Mark all buy levels as filled
        for lvl in grid.levels:
            if lvl.side == LevelSide.BUY:
                lvl.status = LevelStatus.FILLED
        assert geometric_grid.check_rebalance(100100.0, grid) is True


# ============================================================================
# TESTS: Signal Generation
# ============================================================================

class TestSignalGeneration:
    """Test signal generation from GridConfig."""

    def test_generate_start_signal(self, geometric_grid):
        """generate_signal() should produce a valid START_GRID signal."""
        grid = geometric_grid.compute_grid(reference_price=100000.0, symbol="BTCUSDT")
        signal = geometric_grid.generate_signal(grid)

        assert signal.action == SignalAction.START_GRID
        assert signal.symbol == "BTCUSDT"
        assert signal.is_grid_signal()
        assert "grid_id" in signal.metadata
        assert "levels" in signal.metadata
        assert len(signal.metadata["levels"]) == 20

    def test_generate_stop_signal(self, geometric_grid):
        """generate_stop_signal() should produce a STOP_GRID signal with reason."""
        grid = geometric_grid.compute_grid(reference_price=100000.0, symbol="BTCUSDT")
        signal = geometric_grid.generate_stop_signal(grid, reason="manual_override")

        assert signal.action == SignalAction.STOP_GRID
        assert signal.metadata["reason"] == "manual_override"

    def test_generate_pause_signal(self, geometric_grid):
        """generate_pause_signal() should produce a PAUSE_GRID signal."""
        grid = geometric_grid.compute_grid(reference_price=100000.0, symbol="BTCUSDT")
        signal = geometric_grid.generate_pause_signal(grid)

        assert signal.action == SignalAction.PAUSE_GRID


# ============================================================================
# TESTS: GridConfig Serialization
# ============================================================================

class TestGridConfigSerialization:
    """Test GridConfig dict round-trip."""

    def test_gridconfig_to_dict_and_back(self, btc_grid_config):
        """GridConfig → to_dict() → from_dict() → identical."""
        data = btc_grid_config.to_dict()
        restored = GridConfig.from_dict(data)

        assert restored.grid_id == btc_grid_config.grid_id
        assert restored.symbol == btc_grid_config.symbol
        assert restored.grid_type == btc_grid_config.grid_type
        assert restored.reference_price == btc_grid_config.reference_price
        assert restored.upper_bound == btc_grid_config.upper_bound
        assert restored.lower_bound == btc_grid_config.lower_bound
        assert len(restored.levels) == len(btc_grid_config.levels)
        assert restored.profit_per_grid_pct == btc_grid_config.profit_per_grid_pct
        assert restored.total_capital == btc_grid_config.total_capital

    def test_grid_level_to_dict(self, btc_grid_config):
        """Each level should serialize correctly."""
        level_dict = btc_grid_config.levels[0].to_dict()
        assert "level_index" in level_dict
        assert "side" in level_dict
        assert "price" in level_dict
        assert "quantity" in level_dict
        assert "take_profit_price" in level_dict
        assert "status" in level_dict


# ============================================================================
# TESTS: Dynamic Bound Optimization
# ============================================================================

class TestOptimizeBounds:
    """Test ATR-based dynamic bound computation."""

    def test_optimize_bounds_returns_positive_values(self, geometric_grid, sample_ohlcv):
        """Optimize bounds should return positive percentage values."""
        lower, upper = geometric_grid.optimize_bounds(sample_ohlcv)
        assert lower > 0, f"lower bound {lower} should be > 0"
        assert upper > 0, f"upper bound {upper} should be > 0"

    def test_optimize_bounds_respects_floor(self, geometric_grid, sample_ohlcv):
        """Computed bounds should be at least the configured minimum."""
        lower, upper = geometric_grid.optimize_bounds(sample_ohlcv)
        assert lower >= geometric_grid.lower_bound_pct
        assert upper >= geometric_grid.upper_bound_pct

    def test_optimize_bounds_with_short_history(self, geometric_grid):
        """With less than ATR period of data, fall back to defaults."""
        short_data = [{"high": 101000, "low": 99000, "close": 100000}]
        lower, upper = geometric_grid.optimize_bounds(short_data)
        assert lower == geometric_grid.lower_bound_pct
        assert upper == geometric_grid.upper_bound_pct


# ============================================================================
# TESTS: Edge Cases & Error Handling
# ============================================================================

class TestEdgeCases:
    """Test error handling and edge cases."""

    def test_invalid_grid_type_raises(self):
        """Unknown grid type should raise ValueError."""
        with pytest.raises(ValueError, match="grid_type"):
            GridStrategy(grid_type="triangular")  # type: ignore

    def test_zero_levels_raises(self):
        """Zero grid levels should raise ValueError."""
        with pytest.raises(ValueError, match="grid_levels"):
            GridStrategy(grid_levels=0)

    def test_negative_price_raises(self, geometric_grid):
        """Reference price <= 0 should raise ValueError."""
        with pytest.raises(ValueError, match="reference_price"):
            geometric_grid.compute_grid(reference_price=0.0, symbol="BTCUSDT")

        with pytest.raises(ValueError, match="reference_price"):
            geometric_grid.compute_grid(reference_price=-50000.0, symbol="BTCUSDT")

    def test_profit_too_low_raises(self):
        """Profit % below estimated fees should raise ValueError."""
        with pytest.raises(ValueError, match="too low"):
            GridStrategy(
                profit_per_grid_pct=0.05,  # 0.05% profit, but 0.1% estimated fee
                estimated_fee_pct=0.1,
            )

    def test_grid_id_is_deterministic(self, geometric_grid):
        """Same parameters → same grid_id."""
        g1 = geometric_grid.compute_grid(reference_price=100000.0, symbol="BTCUSDT")
        g2 = geometric_grid.compute_grid(reference_price=100000.0, symbol="BTCUSDT")
        assert g1.grid_id == g2.grid_id

    def test_grid_id_differs_for_different_price(self, geometric_grid):
        """Different reference price → different grid_id."""
        g1 = geometric_grid.compute_grid(reference_price=100000.0, symbol="BTCUSDT")
        g2 = geometric_grid.compute_grid(reference_price=101000.0, symbol="BTCUSDT")
        assert g1.grid_id != g2.grid_id

    def test_atr_expands_bounds(self, geometric_grid):
        """Providing ATR should expand grid bounds."""
        without_atr = geometric_grid.compute_grid(reference_price=100000.0, symbol="BTCUSDT")
        with_atr = geometric_grid.compute_grid(
            reference_price=100000.0, symbol="BTCUSDT", atr=2000.0
        )
        # With ATR=2000, bounds should be at least (2000*2/100000)*100 = 4%,
        # so the range should be wider than the default 5%
        range_without = without_atr.upper_bound - without_atr.lower_bound
        range_with = with_atr.upper_bound - with_atr.lower_bound
        assert range_with >= range_without, (
            f"ATR should expand bounds: without={range_without}, with={range_with}"
        )

    def test_compute_rebalance_price(self, geometric_grid, sample_ohlcv):
        """Rebalance price should be near the average of recent candles."""
        ref = geometric_grid.compute_rebalance_price(sample_ohlcv, window=10)
        recent_closes = [c["close"] for c in sample_ohlcv[-10:]]
        expected = sum(recent_closes) / len(recent_closes)
        assert abs(ref - expected) < 0.01

    def test_grid_metrics_are_computable(self, btc_grid_config, geometric_grid):
        """Performance estimation should return sensible numbers."""
        metrics = geometric_grid.estimate_grid_performance(btc_grid_config, volatility_pct=3.0)
        assert metrics["avg_profit_per_trade"] > 0
        assert metrics["estimated_daily_trades"] > 0
        assert metrics["annualized_return_pct"] > 0
        assert 0 < metrics["capital_utilization_pct"] <= 100
        assert metrics["break_even_after_trades"] >= 1


# ============================================================================
# TESTS: Decoupling Verification (CRITICAL)
# ============================================================================

class TestDecoupling:
    """
    Verify that strategy/ does NOT import execution/ or core/.

    This is a hard architectural constraint. The brain must never
    directly call the hands or access the ledger.
    """

    def test_strategy_does_not_import_execution(self):
        """strategy/ must not import from execution/."""
        import strategy.signal
        import strategy.grid_strategy

        # Check module-level imports
        for mod in (strategy.signal, strategy.grid_strategy):
            for attr_name in dir(mod):
                attr = getattr(mod, attr_name)
                if hasattr(attr, "__module__"):
                    mod_name = attr.__module__
                    assert "execution" not in mod_name, (
                        f"{mod.__name__} imports {mod_name} from execution/"
                    )

    def test_strategy_does_not_import_core(self):
        """strategy/ must not import from core/."""
        import strategy.signal
        import strategy.grid_strategy

        for mod in (strategy.signal, strategy.grid_strategy):
            for attr_name in dir(mod):
                attr = getattr(mod, attr_name)
                if hasattr(attr, "__module__"):
                    mod_name = attr.__module__
                    assert "core" not in mod_name, (
                        f"{mod.__name__} imports {mod_name} from core/"
                    )
