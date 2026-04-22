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
    max_spread_rel: float | None = None,
    max_spread_hard: float | None = None,
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

    effective_max_spread = max_spread
    if max_spread_rel is not None and max_spread_rel > 0 and midpoint > 0:
        effective_max_spread = max(effective_max_spread, midpoint * max_spread_rel)
    if max_spread_hard is not None and max_spread_hard > 0:
        effective_max_spread = min(effective_max_spread, max_spread_hard)

    if spread > effective_max_spread:
        rel_text = ""
        if midpoint > 0:
            rel_text = f" ({spread / midpoint:.2%} of midpoint)"
        return PolicyDecision(
            False,
            (
                f"spread {spread:.4f}{rel_text} above "
                f"max_allowed_spread {effective_max_spread:.4f}"
            ),
        )

    if leader_budget_usd < min_order_size_usd:
        return PolicyDecision(False, "leader budget below min order size")

    return PolicyDecision(True, "ok")
