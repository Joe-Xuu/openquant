"""
Risk Module — Independent Pre-Execution Safety Layer.

Public API:
    - RiskGuard, RiskVerdict, Verdict, CircuitBreakerState (from risk.risk_guard)
"""

from risk.risk_guard import CircuitBreakerState, RiskGuard, RiskVerdict, Verdict

__all__ = [
    "RiskGuard",
    "RiskVerdict",
    "Verdict",
    "CircuitBreakerState",
]
