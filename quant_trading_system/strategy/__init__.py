"""
Strategy Module — Multi-Factor Scoring & Trade Signal Generation.

This is the "Brain" of the trading system. It receives market data from the
data/ module, scores the current regime, and emits standardized StrategySignal
objects consumed by main.py (the event bus).

CRITICAL CONSTRAINT:
    This module MUST NOT import from execution/ or core/.
    It only computes numbers and returns StrategySignal dataclasses.

Public API:
    - StrategySignal, SignalAction, SignalBuilder (from strategy.signal)
    - GridStrategy, GridConfig, GridLevel (from strategy.grid_strategy)
    - TrendStrategy, TrendState, TrendDirection (from strategy.trend_strategy)
    - RegimeDetector, MarketRegime, RegimeResult (from strategy.regime_detector)
"""

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
from strategy.trend_strategy import TrendDirection, TrendState, TrendStrategy
from strategy.regime_detector import (
    FactorScores,
    MarketRegime,
    RegimeDetector,
    RegimeResult,
)

__all__ = [
    # Signal contract
    "StrategySignal",
    "SignalAction",
    "SignalBuilder",
    # Grid strategy
    "GridStrategy",
    "GridConfig",
    "GridLevel",
    "GridType",
    "GridStatus",
    "LevelSide",
    "LevelStatus",
    # Trend strategy
    "TrendStrategy",
    "TrendState",
    "TrendDirection",
    # Regime detector
    "RegimeDetector",
    "MarketRegime",
    "RegimeResult",
    "FactorScores",
]
