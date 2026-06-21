"""
Core Module — State Machine & Local Double-Entry Ledger.

Public API:
    - LedgerEngine, get_ledger, reset_ledger (from core.local_ledger)
    - TradingStateMachine, SystemState, StateTransition (from core.state_machine)
"""

from core.local_ledger import LedgerEngine, TradeStatus, get_ledger, reset_ledger
from core.state_machine import (
    StateTransition,
    SystemState,
    TradingStateMachine,
)

__all__ = [
    # Ledger
    "LedgerEngine",
    "TradeStatus",
    "get_ledger",
    "reset_ledger",
    # State Machine
    "TradingStateMachine",
    "SystemState",
    "StateTransition",
]
