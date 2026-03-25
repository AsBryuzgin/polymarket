from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PolicyDecision:
    allowed: bool
    reason: str


def evaluate_order_policy(
    side: str,
    midpoint: float | None,
    spread: float | None,
    leader_budget_usd: float,
    buy_min_price: float,
    buy_max_price: float,
    sell_min_price: float,
    sell_max_price: float,
    max_spread: float,
    min_order_size_usd: float,
) -> PolicyDecision:
    if midpoint is None:
        return PolicyDecision(False, "midpoint is missing")

    side = side.upper()

    if side == "BUY":
        if midpoint < buy_min_price:
            return PolicyDecision(False, f"midpoint {midpoint:.4f} below min_price {buy_min_price:.4f}")

        if midpoint > buy_max_price:
            return PolicyDecision(False, f"midpoint {midpoint:.4f} above max_price {buy_max_price:.4f}")

    elif side == "SELL":
        # Для SELL price-limit вообще не применяем
        pass
    else:
        return PolicyDecision(False, f"unsupported side: {side}")

    if spread is None:
        return PolicyDecision(False, "spread is missing")

    if spread > max_spread:
        return PolicyDecision(False, f"spread {spread:.4f} above max_spread {max_spread:.4f}")

    if leader_budget_usd < min_order_size_usd:
        return PolicyDecision(False, "leader budget below min order size")

    return PolicyDecision(True, "ok")
