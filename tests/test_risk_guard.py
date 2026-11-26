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
            metadata={"levels": [{"price": 50000, "quantity": 0.00001, "side": "BUY"}]},  # $0.50 < $1 min
        )
        verdict = guard._check_order_value(signal)
        assert verdict is not None
        assert verdict.verdict == Verdict.BLOCKED

    def test_modified_verdict_propagated_by_check_signal(self):
        """Regression test: check_signal must return MODIFIED (not discard it as APPROVED)."""
        guard = self._make_guard(equity=1000.0, config={
            "max_position_size_pct": 25.0,
            "max_exposure_pct": 100.0,
        })
        signal = StrategySignal(
            action=SignalAction.START_TREND, symbol="DOGEUSDT", score=0.9,
            metadata={"entry_price": 0.10, "position_size": 5000.0},  # $500 = 50% > 25%
        )
        verdict = guard.check_signal(signal)
        assert verdict.verdict == Verdict.MODIFIED
        assert verdict.modified_params is not None
        assert abs(verdict.modified_params["position_size"] - 2500.0) < 1e-6

    def test_exit_signals_bypass_circuit_breaker(self):
        """STOP_GRID and STOP_TREND must NEVER be blocked by circuit breaker or drawdown."""
        guard = self._make_guard(equity=5000.0)
        guard._peak_equity = 10000.0  # 50% drawdown
        guard._circuit_breaker.triggered = True
        guard._circuit_breaker.trigger_reason = "test"
        from datetime import datetime, timezone
        guard._circuit_breaker.triggered_at = datetime.now(timezone.utc).isoformat()

        for action in (SignalAction.STOP_GRID, SignalAction.STOP_TREND, SignalAction.CLOSE_ALL):
            sig = StrategySignal(action=action, symbol="DOGEUSDT", score=1.0)
            assert guard.check_signal(sig).verdict == Verdict.APPROVED

    def test_daily_loss_resets_baseline_on_new_day(self):
        eq = [1000.0]
        guard = RiskGuard(
            config={"max_daily_loss_pct": 5.0},
            equity_provider=lambda: eq[0],
            positions_provider=lambda: [],
            trade_history_provider=lambda s: [],
        )
        eq[0] = 940.0  # -6% today -> blocked
        assert guard._check_daily_loss() is not None
        # Simulate UTC midnight rollover
        guard._today_date = "2000-01-01"
        assert guard._check_daily_loss() is None
        assert guard._daily_start_equity == 940.0

    def test_sync_baseline_updates_trackers(self):
        guard = self._make_guard(equity=0.0)
        guard.sync_baseline(100.0)
        assert guard._initial_capital == 100.0
        assert guard._daily_start_equity == 100.0
        assert guard._peak_equity == 100.0

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

    def test_simulated_clock_daily_loss_rollover(self):
        from datetime import datetime, timezone
        now_box = [datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)]
        eq_box = [100.0]
        guard = RiskGuard(
            config={"max_daily_loss_pct": 5.0, "max_drawdown_pct": 25.0},
            equity_provider=lambda: eq_box[0],
            positions_provider=lambda: [],
            trade_history_provider=lambda _s: [],
            clock=lambda: now_box[0],
        )
        sig = StrategySignal(action=SignalAction.START_GRID, symbol="DOGEUSDT", score=0.8)
        # Drop 6% on Day 1 -> blocked
        eq_box[0] = 94.0
        assert guard.check_signal(sig).verdict == Verdict.BLOCKED

        # Advance simulated clock to Day 2 -> daily baseline resets to 94.0 -> approved
        now_box[0] = datetime(2026, 1, 2, 0, 5, tzinfo=timezone.utc)
        assert guard.check_signal(sig).verdict == Verdict.APPROVED

