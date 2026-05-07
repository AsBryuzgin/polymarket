from __future__ import annotations

from collections import defaultdict
from typing import Any

from backtest.simulator import safe_float


def summarize_replay_events(events: list[dict[str, Any]], key_field: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        grouped[str(event.get(key_field) or "UNKNOWN")].append(event)

    rows: list[dict[str, Any]] = []
    for key, items in grouped.items():
        entries = [event for event in items if event.get("replay_event_type") == "ENTRY"]
        exits = [event for event in items if event.get("replay_event_type") == "EXIT"]
        final_rows = [event for event in items if event.get("is_final_state_row")]
        realized = sum(safe_float(event.get("realized_pnl_usd")) for event in exits)
        open_positions = sum(
            1 for event in final_rows if safe_float(event.get("position_after_usd")) > 0
        )

        rows.append(
            {
                key_field: key,
                "events": len(items),
                "entries": len(entries),
                "exits": len(exits),
                "realized_pnl_usd": round(realized, 6),
                "open_positions": open_positions,
            }
        )

    rows.sort(key=lambda row: (row["realized_pnl_usd"], row["entries"]), reverse=True)
    return rows


def compute_replay_metrics(events: list[dict[str, Any]]) -> dict[str, Any]:
    entries = [event for event in events if event.get("replay_event_type") == "ENTRY"]
    exits = [event for event in events if event.get("replay_event_type") == "EXIT"]
    final_rows = [event for event in events if event.get("is_final_state_row")]
    realized_pnls = [safe_float(event.get("realized_pnl_usd")) for event in exits]

    gross_profit = sum(value for value in realized_pnls if value > 0)
    gross_loss = abs(sum(value for value in realized_pnls if value < 0))
    profit_factor = None if gross_loss == 0 else gross_profit / gross_loss
    wins = sum(1 for value in realized_pnls if value > 0)
    losses = sum(1 for value in realized_pnls if value < 0)
    closed_trades = wins + losses

    realized_curve = []
    running_realized = 0.0
    for event in events:
        if event.get("replay_event_type") != "EXIT":
            continue
        running_realized += safe_float(event.get("realized_pnl_usd"))
        realized_curve.append(running_realized)

    peak = 0.0
    max_drawdown = 0.0
    for value in realized_curve:
        peak = max(peak, value)
        max_drawdown = min(max_drawdown, value - peak)

    return {
        "events": len([event for event in events if not event.get("is_final_state_row")]),
        "entries": len(entries),
        "exits": len(exits),
        "open_positions": sum(
            1 for event in final_rows if safe_float(event.get("position_after_usd")) > 0
        ),
        "open_notional_usd": round(
            sum(safe_float(event.get("position_after_usd")) for event in final_rows),
            6,
        ),
        "realized_pnl_usd": round(sum(realized_pnls), 6),
        "gross_profit_usd": round(gross_profit, 6),
        "gross_loss_usd": round(gross_loss, 6),
        "profit_factor": round(profit_factor, 6) if profit_factor is not None else "",
        "win_rate": round(wins / closed_trades, 6) if closed_trades else "",
        "max_realized_drawdown_usd": round(max_drawdown, 6),
    }
