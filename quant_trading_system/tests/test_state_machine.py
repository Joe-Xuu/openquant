"""Tests for TradingStateMachine — state transitions, valid/invalid paths."""
import sys
sys.path.insert(0, ".")

from core.state_machine import (
    StateTransition, SystemState, TradingStateMachine,
)


class TestStateMachine:
    def test_initial_state(self):
        sm = TradingStateMachine()
        assert sm.state == SystemState.IDLE
        assert not sm.is_active

    def test_valid_transition_idle_to_warming(self):
        sm = TradingStateMachine()
        assert sm.transition(StateTransition.DATA_READY)
        assert sm.state == SystemState.WARMING_UP

    def test_invalid_transition_rejected(self):
        sm = TradingStateMachine()
        # Can't go directly from IDLE to ACTIVE_TREND
        assert not sm.transition(StateTransition.REGIME_TRENDING)
        assert sm.state == SystemState.IDLE

    def test_full_lifecycle_grid(self):
        sm = TradingStateMachine()
        assert sm.transition(StateTransition.DATA_READY)
        assert sm.state == SystemState.WARMING_UP
        assert sm.transition(StateTransition.REGIME_RANGING)
        assert sm.state == SystemState.ACTIVE_GRID
        assert sm.is_active

    def test_full_lifecycle_trend(self):
        sm = TradingStateMachine()
        sm.transition(StateTransition.DATA_READY)
        sm.transition(StateTransition.REGIME_TRENDING)
        assert sm.state == SystemState.ACTIVE_TREND

    def test_switch_between_strategies(self):
        sm = TradingStateMachine()
        sm.transition(StateTransition.DATA_READY)
        sm.transition(StateTransition.REGIME_RANGING)
        assert sm.state == SystemState.ACTIVE_GRID
        sm.transition(StateTransition.REGIME_TRENDING)
        assert sm.state == SystemState.ACTIVE_TREND
        sm.transition(StateTransition.REGIME_RANGING)
        assert sm.state == SystemState.ACTIVE_GRID

    def test_pause_and_resume(self):
        sm = TradingStateMachine()
        sm.transition(StateTransition.DATA_READY)
        sm.transition(StateTransition.REGIME_RANGING)
        sm.pause("drawdown_limit")
        assert sm.state == SystemState.PAUSED
        sm.resume()
        assert sm.state == SystemState.WARMING_UP

    def test_emergency_from_any_active(self):
        for state in [SystemState.ACTIVE_GRID, SystemState.ACTIVE_TREND, SystemState.WARMING_UP]:
            sm = TradingStateMachine(initial_state=state)
            assert sm.emergency("test")
            assert sm.state == SystemState.EMERGENCY

    def test_emergency_resolved(self):
        sm = TradingStateMachine(initial_state=SystemState.EMERGENCY)
        sm.emergency_resolved()
        assert sm.state == SystemState.IDLE
        assert not sm.can_trade

    def test_can_trade(self):
        sm = TradingStateMachine()
        assert not sm.can_trade
        sm.transition(StateTransition.DATA_READY)
        assert sm.can_trade

    def test_shutdown_from_any(self):
        for state in SystemState:
            sm = TradingStateMachine(initial_state=state)
            sm.shutdown()
            assert sm.state == SystemState.IDLE

    def test_snapshot(self):
        sm = TradingStateMachine()
        sm.transition(StateTransition.DATA_READY)
        snap = sm.snapshot()
        assert snap.state == "WARMING_UP"
        assert snap.transition_count == 1

    def test_history(self):
        sm = TradingStateMachine()
        sm.transition(StateTransition.DATA_READY)
        sm.transition(StateTransition.REGIME_RANGING)
        history = sm.get_history()
        assert len(history) == 2

    def test_convenience_methods(self):
        sm = TradingStateMachine()
        sm.transition(StateTransition.DATA_READY)
        sm.activate_grid()
        assert sm.state == SystemState.ACTIVE_GRID
        sm.activate_trend()
        assert sm.state == SystemState.ACTIVE_TREND
