from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class CopySizeDecision:
    allowed: bool
    amount_usd: float
    source: str
    reason: str
    details: dict[str, Any] = field(default_factory=dict)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def compute_signal_copy_amount(
    *,
    leader_budget_usd: float,
    leader_trade_notional_usd: float | None,
    leader_trade_notional_copy_fraction: float,
    min_order_size_usd: float,
    max_per_trade_usd: float,
    side: str = "BUY",
    remaining_leader_budget_usd: float | None = None,
    leader_portfolio_value_usd: float | None = None,
    leader_exit_fraction: float | None = None,
    max_leader_trade_budget_fraction: float | None = None,
    allow_notional_fallback: bool = False,
    allow_budget_fallback: bool = False,
    precision: int = 2,
) -> CopySizeDecision:
    configured_budget = _safe_float(leader_budget_usd)
    remaining_budget = (
        _safe_float(remaining_leader_budget_usd)
        if remaining_leader_budget_usd is not None
        else configured_budget
    )
    budget = min(configured_budget, remaining_budget)
    notional = _safe_float(leader_trade_notional_usd)
    copy_fraction = _safe_float(leader_trade_notional_copy_fraction)
    min_order = _safe_float(min_order_size_usd)
    max_trade = _safe_float(max_per_trade_usd)
    portfolio_value = _safe_float(leader_portfolio_value_usd)
    exit_fraction = _safe_float(leader_exit_fraction)
    max_budget_fraction = _safe_float(max_leader_trade_budget_fraction)
    side = side.upper()

    details = {
        "side": side,
        "leader_budget_usd": configured_budget,
        "remaining_leader_budget_usd": remaining_budget,
        "sizing_budget_usd": budget,
        "leader_trade_notional_usd": notional,
        "leader_trade_notional_copy_fraction": copy_fraction,
        "leader_portfolio_value_usd": portfolio_value,
        "leader_exit_fraction": exit_fraction,
        "max_leader_trade_budget_fraction": max_budget_fraction,
        "allow_notional_fallback": bool(allow_notional_fallback),
        "allow_budget_fallback": bool(allow_budget_fallback),
        "min_order_size_usd": min_order,
        "max_per_trade_usd": max_trade,
    }

    if configured_budget <= 0:
        return CopySizeDecision(False, 0.0, "none", "leader budget <= 0", details)

    if budget <= 0:
        return CopySizeDecision(False, 0.0, "none", "remaining leader budget <= 0", details)

    if min_order <= 0:
        return CopySizeDecision(False, 0.0, "none", "min order size <= 0", details)

    if max_trade < min_order:
        return CopySizeDecision(
            False,
            0.0,
            "none",
            "max_per_trade_usd below min_order_size_usd",
            details,
        )

    if budget < min_order:
        return CopySizeDecision(False, 0.0, "none", "remaining leader budget below min order size", details)

    if side == "SELL" and exit_fraction > 0:
        effective_fraction = min(exit_fraction, 1.0)
        raw_amount = budget * effective_fraction
        source = "leader_exit_fraction"
        details = {
            **details,
            "leader_exit_fraction_effective": effective_fraction,
        }
    elif side == "BUY" and notional > 0 and portfolio_value > 0:
        raw_budget_fraction = notional / portfolio_value
        effective_fraction = raw_budget_fraction
        fraction_capped = False
        if max_budget_fraction > 0 and effective_fraction > max_budget_fraction:
            effective_fraction = max_budget_fraction
            fraction_capped = True
        raw_amount = budget * effective_fraction
        source = "leader_trade_budget_fraction"
        details = {
            **details,
            "leader_trade_budget_fraction_raw": raw_budget_fraction,
            "leader_trade_budget_fraction_effective": effective_fraction,
            "leader_trade_budget_fraction_capped": fraction_capped,
        }
    elif notional > 0 and copy_fraction > 0 and allow_notional_fallback:
        raw_amount = notional * copy_fraction
        source = "leader_trade_notional"
    elif allow_budget_fallback:
        raw_amount = min(max_trade, budget)
        source = "fallback_budget"
    else:
        if side == "SELL":
            reason = "leader exit fraction unavailable"
        else:
            reason = "leader portfolio value unavailable"
        return CopySizeDecision(False, 0.0, "none", reason, details)

    if raw_amount <= 0:
        return CopySizeDecision(False, 0.0, source, "computed amount <= 0", details)

    amount = min(max_trade, raw_amount, budget)

    if amount < min_order:
        return CopySizeDecision(False, 0.0, source, "computed amount below min order size", details)

    rounded = round(amount, precision)
    return CopySizeDecision(True, rounded, source, "ok", {**details, "raw_amount_usd": raw_amount})
