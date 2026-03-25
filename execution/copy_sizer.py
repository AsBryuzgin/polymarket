from __future__ import annotations

from dataclasses import dataclass


@dataclass
class SizeDecision:
    allowed: bool
    amount_usd: float
    reason: str


def compute_copy_size(
    leader_budget_usd: float,
    target_trade_fraction: float,
    min_order_size_usd: float,
    max_per_trade_usd: float,
) -> SizeDecision:
    if leader_budget_usd <= 0:
        return SizeDecision(False, 0.0, "leader budget <= 0")

    raw_amount = leader_budget_usd * target_trade_fraction
    amount = min(raw_amount, max_per_trade_usd)

    if amount < min_order_size_usd:
        return SizeDecision(False, 0.0, "computed amount below min order size")

    return SizeDecision(True, round(amount, 2), "ok")
