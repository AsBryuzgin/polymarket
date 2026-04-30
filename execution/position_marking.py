from __future__ import annotations

from typing import Any, Callable

from execution.market_diagnostics import diagnose_market_snapshot_error
from execution.polymarket_executor import fetch_market_snapshot


SnapshotLoader = Callable[[str, str], dict[str, Any]]
DiagnosisLoader = Callable[[str, str], dict[str, Any]]


def _safe_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _qty(position_usd: Any, avg_entry_price: Any) -> float:
    position = _safe_float(position_usd) or 0.0
    entry = _safe_float(avg_entry_price) or 0.0
    return position / entry if entry > 0 else 0.0


def mark_position(
    position: dict[str, Any],
    *,
    snapshot_loader: SnapshotLoader = fetch_market_snapshot,
    diagnosis_loader: DiagnosisLoader = diagnose_market_snapshot_error,
    snapshot_side: str = "SELL",
) -> dict[str, Any]:
    row = dict(position)
    token_id = str(position.get("token_id") or "")
    position_usd = _safe_float(position.get("position_usd")) or 0.0
    avg_entry_price = _safe_float(position.get("avg_entry_price")) or 0.0
    qty = _qty(position_usd, avg_entry_price)

    row.update(
        {
            "qty": qty,
            "midpoint": None,
            "best_bid": None,
            "best_ask": None,
            "mark_value_bid_usd": None,
            "mark_value_mid_usd": None,
            "unrealized_pnl_bid_usd": None,
            "unrealized_pnl_mid_usd": None,
            "snapshot_status": "OK",
            "snapshot_reason": "",
            "snapshot_error": None,
            "mark_source": "ORDERBOOK",
            "settlement_price": None,
            "diagnosis_status": None,
            "diagnosis_label": None,
            "diagnosis_reason": None,
            "diagnosis_action_hint": None,
            "diagnosis_question": None,
            "diagnosis_token_outcome": None,
            "diagnosis_token_winner": None,
        }
    )

    try:
        snapshot = snapshot_loader(token_id, snapshot_side)
        midpoint = _safe_float(snapshot.get("midpoint"))
        best_bid = _safe_float(snapshot.get("best_bid"))
        best_ask = _safe_float(snapshot.get("best_ask"))

        row.update(
            {
                "midpoint": midpoint,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "mark_value_bid_usd": qty * best_bid if best_bid is not None and best_bid >= 0 else None,
                "mark_value_mid_usd": qty * midpoint if midpoint is not None and midpoint >= 0 else None,
            }
        )
    except Exception as exc:
        error_message = str(exc)
        diagnosis = diagnosis_loader(token_id, error_message)
        row.update(
            {
                "snapshot_status": "ERROR",
                "snapshot_reason": error_message,
                "snapshot_error": error_message,
                "mark_source": "UNMARKED",
                "diagnosis_status": diagnosis.get("diagnosis_status"),
                "diagnosis_label": diagnosis.get("diagnosis_label"),
                "diagnosis_reason": diagnosis.get("diagnosis_reason"),
                "diagnosis_action_hint": diagnosis.get("action_hint"),
                "diagnosis_question": diagnosis.get("question"),
                "diagnosis_token_outcome": diagnosis.get("token_outcome"),
                "diagnosis_token_winner": diagnosis.get("token_winner"),
            }
        )

        if diagnosis.get("diagnosis_status") == "NO_ORDERBOOK_CLOSED_OR_RESOLVED":
            winner = diagnosis.get("token_winner")
            if winner is True or winner is False:
                settlement_price = 1.0 if winner else 0.0
                settlement_value = qty * settlement_price
                row.update(
                    {
                        "snapshot_status": "SETTLED",
                        "snapshot_reason": (
                            "settlement fallback from resolved market "
                            f"(winner={winner})"
                        ),
                        "mark_source": "SETTLEMENT",
                        "settlement_price": settlement_price,
                        "midpoint": settlement_price,
                        "best_bid": settlement_price,
                        "best_ask": settlement_price,
                        "mark_value_bid_usd": settlement_value,
                        "mark_value_mid_usd": settlement_value,
                    }
                )

    mark_bid = row.get("mark_value_bid_usd")
    mark_mid = row.get("mark_value_mid_usd")
    row["unrealized_pnl_bid_usd"] = (
        mark_bid - position_usd if isinstance(mark_bid, (int, float)) else None
    )
    row["unrealized_pnl_mid_usd"] = (
        mark_mid - position_usd if isinstance(mark_mid, (int, float)) else None
    )
    return row


def is_marked(row: dict[str, Any]) -> bool:
    return str(row.get("snapshot_status") or "").upper() in {"OK", "SETTLED"}


def is_unmarked(row: dict[str, Any]) -> bool:
    return not is_marked(row)
