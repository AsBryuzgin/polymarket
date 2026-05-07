from __future__ import annotations


def price_drift_ok(
    *,
    leader_price: float | None,
    current_price: float | None,
    side: str,
    max_abs: float,
    max_rel: float,
) -> tuple[bool, str]:
    if leader_price is None or leader_price <= 0:
        return True, "leader trade price missing"
    if current_price is None or current_price <= 0:
        return False, "current price quote missing"

    abs_drift = abs(current_price - leader_price)
    rel_drift = abs_drift / leader_price
    side = side.upper()

    if side == "BUY" and current_price > leader_price:
        if abs_drift > max_abs and rel_drift > max_rel:
            if (abs_drift / max_abs) >= (rel_drift / max_rel):
                return False, f"buy price drift abs too high: {abs_drift:.4f} > {max_abs:.4f}"
            return False, f"buy price drift rel too high: {rel_drift:.4f} > {max_rel:.4f}"

    if side == "SELL" and current_price < leader_price:
        if abs_drift > max_abs and rel_drift > max_rel:
            if (abs_drift / max_abs) >= (rel_drift / max_rel):
                return False, f"sell price drift abs too high: {abs_drift:.4f} > {max_abs:.4f}"
            return False, f"sell price drift rel too high: {rel_drift:.4f} > {max_rel:.4f}"

    return True, "ok"
