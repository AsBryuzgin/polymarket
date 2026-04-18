from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


EPS = 1e-8

FINAL_ATTEMPT_STATUSES = {
    "PREVIEW_READY",
    "PAPER_FILLED",
    "LIVE_FILLED",
    "LIVE_FILLED_RECOVERED",
    "LIVE_FILLED_UNVERIFIED_AMOUNT",
    "LIVE_BLOCKED",
    "LIVE_PREFLIGHT_BLOCKED",
    "LIVE_REJECTED",
    "LIVE_SUBMIT_ERROR",
    "EXECUTION_ERROR",
}


@dataclass(frozen=True)
class ReconciliationReport:
    position_rows: list[dict[str, Any]] = field(default_factory=list)
    issue_rows: list[dict[str, Any]] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)


def _safe_float(value: Any) -> float:
    try:
        return float(value) if value is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def reconstruct_positions_from_trade_history(history_rows: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    reconstructed: dict[tuple[str, str], dict[str, Any]] = {}

    for row in history_rows:
        key = (row["leader_wallet"], row["token_id"])
        state = reconstructed.get(
            key,
            {
                "leader_wallet": row["leader_wallet"],
                "leader_user_name": row.get("leader_user_name"),
                "category": row.get("category"),
                "token_id": row["token_id"],
                "position_usd": 0.0,
                "avg_entry_price": None,
                "entries": 0,
                "exits": 0,
            },
        )

        amount_usd = _safe_float(row.get("amount_usd"))
        price = _safe_float(row.get("price"))
        before = _safe_float(state["position_usd"])

        if row.get("event_type") == "ENTRY" and row.get("side") == "BUY":
            after = before + amount_usd
            old_avg = state["avg_entry_price"]
            if price > 0:
                if old_avg is None or before <= 0:
                    new_avg = price
                else:
                    new_avg = ((before * old_avg) + (amount_usd * price)) / after
            else:
                new_avg = old_avg
            state["position_usd"] = after
            state["avg_entry_price"] = new_avg
            state["entries"] += 1

        elif row.get("event_type") == "EXIT" and row.get("side") == "SELL":
            sell_amount = min(before, amount_usd)
            after = max(before - sell_amount, 0.0)
            state["position_usd"] = after
            if after <= EPS:
                state["avg_entry_price"] = None
            state["exits"] += 1

        reconstructed[key] = state

    return reconstructed


def reconcile_executor_state(
    *,
    trade_history_rows: list[dict[str, Any]],
    open_position_rows: list[dict[str, Any]],
    processed_signal_rows: list[dict[str, Any]],
    order_attempt_rows: list[dict[str, Any]],
    exchange_position_rows: list[dict[str, Any]] | None = None,
    exchange_open_order_rows: list[dict[str, Any]] | None = None,
    external_issue_rows: list[dict[str, Any]] | None = None,
    exchange_position_qty_tolerance: float = 1e-6,
) -> ReconciliationReport:
    reconstructed = reconstruct_positions_from_trade_history(trade_history_rows)
    actual_open = {
        (row["leader_wallet"], row["token_id"]): row
        for row in open_position_rows
    }

    position_rows: list[dict[str, Any]] = []
    issue_rows: list[dict[str, Any]] = list(external_issue_rows or [])

    for key in sorted(set(reconstructed) | set(actual_open)):
        replay = reconstructed.get(key, {})
        actual = actual_open.get(key, {})

        replay_pos = _safe_float(replay.get("position_usd"))
        actual_pos = _safe_float(actual.get("position_usd"))
        position_match = abs(replay_pos - actual_pos) <= EPS

        row = {
            "leader_wallet": key[0],
            "token_id": key[1],
            "replay_position_usd": round(replay_pos, 8),
            "actual_open_position_usd": round(actual_pos, 8),
            "position_match": position_match,
            "replay_avg_entry_price": replay.get("avg_entry_price"),
            "actual_avg_entry_price": actual.get("avg_entry_price"),
            "entries": replay.get("entries", 0),
            "exits": replay.get("exits", 0),
        }
        position_rows.append(row)

        if not position_match:
            issue_rows.append(
                {
                    "issue_type": "POSITION_MISMATCH",
                    "severity": "ERROR",
                    "leader_wallet": key[0],
                    "token_id": key[1],
                    "details": (
                        f"replay_position_usd={replay_pos:.8f}, "
                        f"actual_open_position_usd={actual_pos:.8f}"
                    ),
                }
            )

    for row in processed_signal_rows:
        if row.get("status") == "PROCESSING":
            issue_rows.append(
                {
                    "issue_type": "SIGNAL_STUCK_PROCESSING",
                    "severity": "ERROR",
                    "signal_id": row.get("signal_id"),
                    "leader_wallet": row.get("leader_wallet"),
                    "token_id": row.get("token_id"),
                    "details": row.get("reason") or "",
                }
            )

    attempts_by_signal: dict[str, list[dict[str, Any]]] = {}
    for row in order_attempt_rows:
        attempts_by_signal.setdefault(row["signal_id"], []).append(row)
        if row.get("status") not in FINAL_ATTEMPT_STATUSES:
            issue_rows.append(
                {
                    "issue_type": "ORDER_ATTEMPT_NOT_FINAL",
                    "severity": "ERROR",
                    "signal_id": row.get("signal_id"),
                    "leader_wallet": row.get("leader_wallet"),
                    "token_id": row.get("token_id"),
                    "details": f"attempt_id={row.get('attempt_id')} status={row.get('status')}",
                }
            )

    for row in processed_signal_rows:
        status = str(row.get("status") or "")
        signal_id = row.get("signal_id")
        if status.startswith(("PREVIEW_READY", "PAPER_FILLED", "LIVE_FILLED")) and signal_id not in attempts_by_signal:
            issue_rows.append(
                {
                    "issue_type": "FILLED_SIGNAL_WITHOUT_ORDER_ATTEMPT",
                    "severity": "ERROR",
                    "signal_id": signal_id,
                    "leader_wallet": row.get("leader_wallet"),
                    "token_id": row.get("token_id"),
                    "details": f"processed signal status={status}",
                }
            )

    if exchange_position_rows is not None:
        local_by_token: dict[str, dict[str, Any]] = {}
        for row in open_position_rows:
            token_id = row["token_id"]
            position_usd = _safe_float(row.get("position_usd"))
            avg_entry = _safe_float(row.get("avg_entry_price"))
            qty = position_usd / avg_entry if avg_entry > 0 else 0.0
            bucket = local_by_token.setdefault(
                token_id,
                {
                    "token_id": token_id,
                    "local_qty": 0.0,
                    "local_cost_usd": 0.0,
                    "local_rows": 0,
                },
            )
            bucket["local_qty"] += qty
            bucket["local_cost_usd"] += position_usd
            bucket["local_rows"] += 1

        exchange_by_token: dict[str, dict[str, Any]] = {}
        for row in exchange_position_rows:
            token_id = str(row["token_id"])
            qty = _safe_float(row.get("size"))
            current_value = _safe_float(row.get("current_value_usd"))
            bucket = exchange_by_token.setdefault(
                token_id,
                {
                    "token_id": token_id,
                    "exchange_qty": 0.0,
                    "exchange_current_value_usd": 0.0,
                    "exchange_rows": 0,
                    "qty_available": False,
                },
            )
            if row.get("size") is not None:
                bucket["exchange_qty"] += qty
                bucket["qty_available"] = True
            bucket["exchange_current_value_usd"] += current_value
            bucket["exchange_rows"] += 1

        for token_id in sorted(set(local_by_token) | set(exchange_by_token)):
            local = local_by_token.get(token_id, {})
            exchange = exchange_by_token.get(token_id, {})
            local_qty = _safe_float(local.get("local_qty"))
            exchange_qty = _safe_float(exchange.get("exchange_qty"))
            qty_available = bool(exchange.get("qty_available"))

            if not local and exchange_qty > exchange_position_qty_tolerance:
                issue_rows.append(
                    {
                        "issue_type": "EXCHANGE_POSITION_ONLY",
                        "severity": "WARN",
                        "token_id": token_id,
                        "details": f"exchange_qty={exchange_qty:.8f}, no local open position",
                    }
                )
            elif local and not exchange:
                issue_rows.append(
                    {
                        "issue_type": "LOCAL_POSITION_NOT_ON_EXCHANGE",
                        "severity": "WARN",
                        "token_id": token_id,
                        "details": f"local_qty={local_qty:.8f}, no exchange position row",
                    }
                )
            elif qty_available and abs(local_qty - exchange_qty) > exchange_position_qty_tolerance:
                issue_rows.append(
                    {
                        "issue_type": "EXCHANGE_POSITION_QTY_MISMATCH",
                        "severity": "ERROR",
                        "token_id": token_id,
                        "details": f"local_qty={local_qty:.8f}, exchange_qty={exchange_qty:.8f}",
                    }
                )

    for row in exchange_open_order_rows or []:
        remaining_size = _safe_float(row.get("remaining_size"))
        if remaining_size <= EPS:
            continue
        issue_rows.append(
            {
                "issue_type": "EXCHANGE_OPEN_ORDER_PRESENT",
                "severity": "WARN",
                "token_id": row.get("token_id"),
                "details": (
                    f"order_id={row.get('order_id')} side={row.get('side')} "
                    f"remaining_size={remaining_size:.8f}"
                ),
            }
        )

    summary = {
        "positions_checked": len(position_rows),
        "issues": len(issue_rows),
        "position_mismatches": sum(1 for row in issue_rows if row["issue_type"] == "POSITION_MISMATCH"),
        "stuck_processing_signals": sum(1 for row in issue_rows if row["issue_type"] == "SIGNAL_STUCK_PROCESSING"),
        "nonfinal_order_attempts": sum(1 for row in issue_rows if row["issue_type"] == "ORDER_ATTEMPT_NOT_FINAL"),
        "exchange_position_issues": sum(1 for row in issue_rows if row["issue_type"].startswith("EXCHANGE_POSITION") or row["issue_type"] == "LOCAL_POSITION_NOT_ON_EXCHANGE"),
        "exchange_open_orders": sum(1 for row in issue_rows if row["issue_type"] == "EXCHANGE_OPEN_ORDER_PRESENT"),
        "exchange_fetch_issues": sum(1 for row in issue_rows if row["issue_type"] == "EXCHANGE_FETCH_ERROR"),
    }

    return ReconciliationReport(
        position_rows=position_rows,
        issue_rows=issue_rows,
        summary=summary,
    )
