from __future__ import annotations

from dataclasses import dataclass
from typing import Any


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None and value != "" else default
    except (TypeError, ValueError):
        return default


@dataclass
class SimulationResult:
    event_rows: list[dict[str, Any]]
    skipped_rows: list[dict[str, Any]]
    final_positions: dict[tuple[str, str], dict[str, Any]]


def _base_event(fill: dict[str, Any]) -> dict[str, Any]:
    return {
        "observation_id": fill.get("observation_id", ""),
        "observed_at": fill.get("observed_at", ""),
        "leader_wallet": fill.get("leader_wallet"),
        "leader_user_name": fill.get("leader_user_name"),
        "category": fill.get("category"),
        "token_id": fill.get("token_id"),
        "source_latest_status": fill.get("source_latest_status", ""),
        "selected_signal_id": fill.get("selected_signal_id", ""),
        "selected_side": fill.get("side", ""),
        "selected_trade_notional_usd": fill.get("selected_trade_notional_usd", ""),
        "sizing_source": fill.get("sizing_source", ""),
    }


def simulate_position_fills(fill_rows: list[dict[str, Any]]) -> SimulationResult:
    positions: dict[tuple[str, str], dict[str, Any]] = {}
    event_rows: list[dict[str, Any]] = []
    skipped_rows: list[dict[str, Any]] = []

    for fill in fill_rows:
        leader_wallet = fill.get("leader_wallet")
        token_id = fill.get("token_id")
        side = str(fill.get("side") or "").upper()
        amount_usd = safe_float(fill.get("amount_usd"))
        exec_price = safe_float(fill.get("exec_price"))

        if not leader_wallet or not token_id:
            skipped_rows.append({**_base_event(fill), "skip_reason": "missing position key"})
            continue

        if side not in {"BUY", "SELL"}:
            skipped_rows.append({**_base_event(fill), "skip_reason": f"unsupported side: {side}"})
            continue

        if amount_usd <= 0:
            skipped_rows.append({**_base_event(fill), "skip_reason": "amount_usd <= 0"})
            continue

        if exec_price <= 0:
            skipped_rows.append({**_base_event(fill), "skip_reason": "exec_price <= 0"})
            continue

        key = (str(leader_wallet), str(token_id))
        position = positions.get(
            key,
            {
                "leader_wallet": str(leader_wallet),
                "leader_user_name": fill.get("leader_user_name"),
                "category": fill.get("category"),
                "token_id": str(token_id),
                "position_usd": 0.0,
                "avg_entry_price": None,
                "realized_pnl_usd": 0.0,
            },
        )

        position_before = safe_float(position.get("position_usd"))
        avg_entry_before = position.get("avg_entry_price")
        base = _base_event(fill)

        if side == "BUY":
            position_after = position_before + amount_usd
            if avg_entry_before is None or position_before <= 0:
                avg_entry_after = exec_price
            else:
                avg_entry_after = (
                    (position_before * float(avg_entry_before)) + (amount_usd * exec_price)
                ) / position_after

            position["position_usd"] = position_after
            position["avg_entry_price"] = avg_entry_after

            event_rows.append(
                {
                    **base,
                    "amount_usd": round(amount_usd, 6),
                    "exec_price": exec_price,
                    "position_before_usd": round(position_before, 6),
                    "position_after_usd": round(position_after, 6),
                    "avg_entry_after": round(avg_entry_after, 6),
                    "replay_event_type": "ENTRY",
                    "realized_pnl_usd": "",
                    "is_final_state_row": False,
                }
            )

        else:
            if position_before <= 0:
                skipped_rows.append({**base, "skip_reason": "sell without open replay position"})
                continue

            sell_amount = min(position_before, amount_usd)
            position_after = position_before - sell_amount

            realized_pnl_usd = ""
            if avg_entry_before is not None and float(avg_entry_before) > 0:
                realized_pnl_usd = round(
                    sell_amount * ((exec_price - float(avg_entry_before)) / float(avg_entry_before)),
                    6,
                )
                position["realized_pnl_usd"] = safe_float(position["realized_pnl_usd"]) + float(
                    realized_pnl_usd
                )

            position["position_usd"] = max(position_after, 0.0)
            if position["position_usd"] == 0:
                position["avg_entry_price"] = None

            event_rows.append(
                {
                    **base,
                    "amount_usd": round(sell_amount, 6),
                    "exec_price": exec_price,
                    "position_before_usd": round(position_before, 6),
                    "position_after_usd": round(position["position_usd"], 6),
                    "avg_entry_after": (
                        round(position["avg_entry_price"], 6)
                        if position["avg_entry_price"] is not None
                        else ""
                    ),
                    "replay_event_type": "EXIT",
                    "realized_pnl_usd": realized_pnl_usd,
                    "is_final_state_row": False,
                }
            )

        positions[key] = position

    for (_leader_wallet, _token_id), position in positions.items():
        event_rows.append(
            {
                "observation_id": "",
                "observed_at": "",
                "leader_wallet": position["leader_wallet"],
                "leader_user_name": position.get("leader_user_name"),
                "category": position.get("category"),
                "token_id": position["token_id"],
                "source_latest_status": "",
                "selected_signal_id": "",
                "selected_side": "",
                "selected_trade_notional_usd": "",
                "sizing_source": "",
                "amount_usd": "",
                "exec_price": "",
                "position_before_usd": "",
                "position_after_usd": round(safe_float(position.get("position_usd")), 6),
                "avg_entry_after": (
                    round(position["avg_entry_price"], 6)
                    if position.get("avg_entry_price") is not None
                    else ""
                ),
                "replay_event_type": "FINAL_STATE",
                "realized_pnl_usd": round(safe_float(position.get("realized_pnl_usd")), 6),
                "is_final_state_row": True,
            }
        )

    return SimulationResult(event_rows, skipped_rows, positions)
