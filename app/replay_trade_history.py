from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path
from pprint import pprint

from execution.state_store import init_db, list_trade_history, list_open_positions


OUT_FILE = Path("data/replay_trade_history_report.csv")


def _safe_float(x) -> float:
    try:
        return float(x) if x is not None else 0.0
    except Exception:
        return 0.0


def save_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        print("No replay report rows to save.")
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved: {path}")


def main() -> None:
    init_db()

    # oldest -> newest for replay
    history = list(reversed(list_trade_history(limit=100000)))
    actual_open_positions = list_open_positions(limit=100000)

    reconstructed: dict[tuple[str, str], dict] = {}
    report_rows: list[dict] = []
    errors: list[dict] = []

    for row in history:
        key = (row["leader_wallet"], row["token_id"])
        side = row["side"]
        event_type = row["event_type"]
        amount_usd = _safe_float(row.get("amount_usd"))
        price = _safe_float(row.get("price"))

        state = reconstructed.get(
            key,
            {
                "leader_wallet": row["leader_wallet"],
                "leader_user_name": row.get("leader_user_name"),
                "category": row.get("category"),
                "token_id": row["token_id"],
                "position_usd": 0.0,
                "avg_entry_price": None,
                "realized_pnl_usd": 0.0,
                "entries": 0,
                "exits": 0,
            },
        )

        before = float(state["position_usd"])

        if event_type == "ENTRY" and side == "BUY":
            new_position = before + amount_usd
            old_avg = state["avg_entry_price"]

            if price > 0:
                if old_avg is None or before <= 0:
                    new_avg = price
                else:
                    new_avg = ((before * old_avg) + (amount_usd * price)) / new_position
            else:
                new_avg = old_avg

            state["position_usd"] = new_position
            state["avg_entry_price"] = new_avg
            state["entries"] += 1

        elif event_type == "EXIT" and side == "SELL":
            if before <= 0:
                errors.append(
                    {
                        "type": "EXIT_WITHOUT_OPEN_POSITION",
                        "leader_wallet": row["leader_wallet"],
                        "token_id": row["token_id"],
                        "event_id": row["event_id"],
                        "event_time": row["event_time"],
                        "amount_usd": amount_usd,
                    }
                )
            else:
                sell_amount = min(before, amount_usd)
                entry_avg = state["avg_entry_price"]

                if entry_avg is not None and price > 0:
                    pnl_pct = (price - entry_avg) / entry_avg
                    realized = sell_amount * pnl_pct
                    state["realized_pnl_usd"] += realized

                after = before - sell_amount
                state["position_usd"] = max(after, 0.0)
                if state["position_usd"] == 0:
                    state["avg_entry_price"] = None

                state["exits"] += 1

        else:
            errors.append(
                {
                    "type": "UNSUPPORTED_EVENT",
                    "leader_wallet": row["leader_wallet"],
                    "token_id": row["token_id"],
                    "event_id": row["event_id"],
                    "event_time": row["event_time"],
                    "side": side,
                    "event_type": event_type,
                }
            )

        if state["position_usd"] < -1e-9:
            errors.append(
                {
                    "type": "NEGATIVE_POSITION",
                    "leader_wallet": row["leader_wallet"],
                    "token_id": row["token_id"],
                    "event_id": row["event_id"],
                    "event_time": row["event_time"],
                    "position_usd": state["position_usd"],
                }
            )

        reconstructed[key] = state

        report_rows.append(
            {
                "event_id": row["event_id"],
                "event_time": row["event_time"],
                "leader_user_name": row.get("leader_user_name"),
                "category": row.get("category"),
                "leader_wallet": row["leader_wallet"],
                "token_id": row["token_id"],
                "side": side,
                "event_type": event_type,
                "amount_usd": amount_usd,
                "price": price,
                "position_after_replay_usd": round(state["position_usd"], 6),
                "avg_entry_after_replay": round(state["avg_entry_price"], 6) if state["avg_entry_price"] is not None else "",
                "realized_pnl_after_replay_usd": round(state["realized_pnl_usd"], 6),
            }
        )

    actual_map = {
        (row["leader_wallet"], row["token_id"]): row
        for row in actual_open_positions
    }

    reconciliation_rows = []
    all_keys = set(reconstructed.keys()) | set(actual_map.keys())

    for key in sorted(all_keys):
        replay_state = reconstructed.get(key, {})
        actual_state = actual_map.get(key, {})

        replay_pos = _safe_float(replay_state.get("position_usd"))
        actual_pos = _safe_float(actual_state.get("position_usd"))

        reconciliation_rows.append(
            {
                "leader_wallet": key[0],
                "token_id": key[1],
                "leader_user_name": replay_state.get("leader_user_name") or actual_state.get("leader_user_name"),
                "category": replay_state.get("category") or actual_state.get("category"),
                "replay_position_usd": round(replay_pos, 6),
                "actual_open_position_usd": round(actual_pos, 6),
                "position_match": abs(replay_pos - actual_pos) < 1e-9,
                "replay_avg_entry_price": round(replay_state.get("avg_entry_price"), 6) if replay_state.get("avg_entry_price") is not None else "",
                "actual_avg_entry_price": actual_state.get("avg_entry_price", ""),
                "replay_realized_pnl_usd": round(_safe_float(replay_state.get("realized_pnl_usd")), 6),
                "entries": replay_state.get("entries", 0),
                "exits": replay_state.get("exits", 0),
            }
        )

    print("=== REPLAY TRADE HISTORY REPORT ===")
    pprint(report_rows[-20:])

    print("\n=== RECONCILIATION ===")
    pprint(reconciliation_rows)

    print("\n=== ERRORS ===")
    pprint(errors)

    save_csv(report_rows, OUT_FILE)

    mismatches = [r for r in reconciliation_rows if not r["position_match"]]
    print(f"\nsummary: rows={len(report_rows)} | reconciliation_keys={len(reconciliation_rows)} | mismatches={len(mismatches)} | errors={len(errors)}")


if __name__ == "__main__":
    main()
