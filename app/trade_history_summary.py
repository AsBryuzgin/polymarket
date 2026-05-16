from __future__ import annotations

import csv
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.state_store import init_db, list_trade_history


OUT_LEADER = Path("data/trade_history_summary_by_leader.csv")
OUT_CATEGORY = Path("data/trade_history_summary_by_category.csv")


def safe_float(x) -> float:
    try:
        return float(x) if x is not None else 0.0
    except Exception:
        return 0.0


def safe_int(x) -> int:
    try:
        return int(x) if x is not None else 0
    except Exception:
        return 0


def summarize(rows: list[dict], key_field: str) -> list[dict]:
    grouped = defaultdict(list)
    for row in rows:
        grouped[row.get(key_field) or "UNKNOWN"].append(row)

    result = []

    for key, items in grouped.items():
        entries = [r for r in items if r.get("event_type") == "ENTRY"]
        exits = [r for r in items if r.get("event_type") == "EXIT"]

        realized_pnl_values = [
            safe_float(r.get("realized_pnl_usd"))
            for r in exits
            if r.get("realized_pnl_usd") is not None
        ]

        holding_values = [
            safe_float(r.get("holding_minutes"))
            for r in exits
            if r.get("holding_minutes") is not None
        ]

        win_count = sum(
            1 for r in exits
            if r.get("realized_pnl_usd") is not None and safe_float(r.get("realized_pnl_usd")) > 0
        )

        loss_count = sum(
            1 for r in exits
            if r.get("realized_pnl_usd") is not None and safe_float(r.get("realized_pnl_usd")) < 0
        )

        flat_count = sum(
            1 for r in exits
            if r.get("realized_pnl_usd") is not None and safe_float(r.get("realized_pnl_usd")) == 0
        )

        total_exits = len(exits)
        win_rate = (win_count / total_exits) if total_exits > 0 else None

        gross_entry_usd = sum(safe_float(r.get("gross_value_usd")) for r in entries)
        gross_exit_usd = sum(safe_float(r.get("gross_value_usd")) for r in exits)
        realized_pnl_total = sum(realized_pnl_values)

        avg_holding_minutes = (
            sum(holding_values) / len(holding_values) if holding_values else None
        )

        result.append({
            key_field: key,
            "total_rows": len(items),
            "entries": len(entries),
            "exits": len(exits),
            "gross_entry_usd": round(gross_entry_usd, 4),
            "gross_exit_usd": round(gross_exit_usd, 4),
            "realized_pnl_usd": round(realized_pnl_total, 4),
            "win_count": win_count,
            "loss_count": loss_count,
            "flat_count": flat_count,
            "win_rate": round(win_rate, 6) if win_rate is not None else "",
            "avg_holding_minutes": round(avg_holding_minutes, 4) if avg_holding_minutes is not None else "",
        })

    result.sort(key=lambda r: r["realized_pnl_usd"], reverse=True)
    return result


def save_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        print(f"No rows to save for {path}")
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
    rows = list_trade_history(limit=100000)

    if not rows:
        print("No trade history rows yet.")
        return

    by_leader = summarize(rows, "leader_user_name")
    by_category = summarize(rows, "category")

    save_csv(by_leader, OUT_LEADER)
    save_csv(by_category, OUT_CATEGORY)

    print("\n=== BY LEADER ===")
    for row in by_leader:
        print(row)

    print("\n=== BY CATEGORY ===")
    for row in by_category:
        print(row)


if __name__ == "__main__":
    main()
