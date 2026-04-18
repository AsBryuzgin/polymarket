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
    precision: int = 2,
) -> CopySizeDecision:
    budget = _safe_float(leader_budget_usd)
    notional = _safe_float(leader_trade_notional_usd)
    copy_fraction = _safe_float(leader_trade_notional_copy_fraction)
    min_order = _safe_float(min_order_size_usd)
    max_trade = _safe_float(max_per_trade_usd)

    details = {
        "leader_budget_usd": budget,
        "leader_trade_notional_usd": notional,
        "leader_trade_notional_copy_fraction": copy_fraction,
        "min_order_size_usd": min_order,
        "max_per_trade_usd": max_trade,
    }

    if budget <= 0:
        return CopySizeDecision(False, 0.0, "none", "leader budget <= 0", details)

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
        return CopySizeDecision(False, 0.0, "none", "leader budget below min order size", details)

    if notional > 0 and copy_fraction > 0:
        raw_amount = notional * copy_fraction
        amount = max(min_order, raw_amount)
        amount = min(max_trade, amount, budget)
        source = "leader_trade_notional"
    else:
        raw_amount = min(max_trade, budget)
        amount = max(min_order, raw_amount)
        amount = min(max_trade, amount, budget)
        source = "fallback_budget"

    if amount < min_order:
        return CopySizeDecision(False, 0.0, source, "computed amount below min order size", details)

    rounded = round(amount, precision)
    return CopySizeDecision(True, rounded, source, "ok", {**details, "raw_amount_usd": raw_amount})
