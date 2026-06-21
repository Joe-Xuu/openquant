"""Tests for RiskGuard — drawdown, position limits, circuit breaker, verdicts."""
import sys
sys.path.insert(0, ".")

from strategy.signal import SignalAction, StrategySignal
from risk.risk_guard import RiskGuard, RiskVerdict, Verdict


class TestRiskGuard:
    def _make_guard(self, equity=10000.0, positions=None, trades=None, config=None):
        return RiskGuard(
            config=config or {},
            equity_provider=lambda: equity,
            positions_provider=lambda: positions or [],
            trade_history_provider=lambda s: trades or [],
        )

    def test_emergency_signal_always_approved(self):
        guard = self._make_guard()
        signal = StrategySignal(action=SignalAction.CLOSE_ALL, symbol="BTCUSDT", score=1.0)
        verdict = guard.check_signal(signal)
        assert verdict.verdict == Verdict.APPROVED

    def test_neutral_signal_approved(self):
        guard = self._make_guard()
        signal = StrategySignal(action=SignalAction.NEUTRAL, symbol="BTCUSDT", score=0.5)
        verdict = guard.check_signal(signal)
        assert verdict.verdict == Verdict.APPROVED

    def test_drawdown_block(self):
        guard = self._make_guard(equity=8000.0)  # -20% from peak 10000
        guard.update_peak_equity()
        guard._peak_equity = 10000.0  # Force peak
        signal = StrategySignal(
            action=SignalAction.START_TREND, symbol="BTCUSDT", score=0.9,
            metadata={"entry_price": 100000, "position_size": 0.01},
        )
        verdict = guard.check_signal(signal)
        # 20% DD should be blocked if max_drawdown_pct=15 (default)
        # Unless the peak was never updated
        guard._peak_equity = 10000.0
        guard.max_drawdown_pct = 15.0
        assert guard._check_drawdown() is not None, "Should detect drawdown > 15%"

    def test_dust_order_blocked(self):
        guard = self._make_guard()
        signal = StrategySignal(
            action=SignalAction.START_GRID, symbol="BTCUSDT", score=0.9,
            metadata={"levels": [{"price": 50000, "quantity": 0.0001, "side": "BUY"}]},
        )
        # $5 order should be blocked for dust
        verdict = guard._check_order_value(signal)
        assert verdict is not None
        assert verdict.verdict == Verdict.BLOCKED

    def test_risk_summary(self):
        guard = self._make_guard()
        summary = guard.get_risk_summary()
        assert "current_equity" in summary
        assert "drawdown_pct" in summary
        assert "circuit_breaker_triggered" in summary

    def test_circuit_breaker_consecutive_losses(self):
        guard = self._make_guard()
        for _ in range(6):
            guard.register_trade_result(-100)  # 6 consecutive losses
        assert guard._circuit_breaker.triggered

    def test_circuit_breaker_blocks_new_trades(self):
        guard = self._make_guard()
        guard._circuit_breaker.triggered = True
        guard._circuit_breaker.trigger_reason = "test"
        from datetime import datetime, timezone
        guard._circuit_breaker.triggered_at = datetime.now(timezone.utc).isoformat()

        signal = StrategySignal(
            action=SignalAction.START_TREND, symbol="BTCUSDT", score=0.9,
        )
        verdict = guard.check_signal(signal)
        assert verdict.verdict == Verdict.BLOCKED

    def test_position_size_modification(self):
        guard = self._make_guard(equity=10000)
        guard.max_position_size_pct = 5.0  # Max 5% per position
        signal = StrategySignal(
            action=SignalAction.START_TREND, symbol="BTCUSDT", score=0.9,
            metadata={"entry_price": 100000, "position_size": 0.02},  # $2000 = 20%
        )
        verdict = guard._check_position_size(signal)
        assert verdict is not None, "Should modify position that exceeds 5% limit"

    def test_volatility_spike_detection(self):
        guard = self._make_guard()
        guard.register_volatility_spike(0.05, 0.01)  # 5× average
        assert guard._circuit_breaker.triggered
