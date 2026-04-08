from __future__ import annotations

import csv
import sqlite3
from collections import defaultdict
from pathlib import Path
from statistics import median
from pprint import pprint

from execution.state_store import DB_PATH


OUT_LEADER = Path("data/signal_observation_summary_by_leader.csv")
OUT_CATEGORY = Path("data/signal_observation_summary_by_category.csv")
OUT_STATUS = Path("data/signal_observation_summary_by_status.csv")


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def list_signal_observations(limit: int = 100000) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM signal_observations
        ORDER BY observation_id DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def _safe_float(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


def _median(values, default=""):
    clean = [float(v) for v in values if v is not None]
    return round(median(clean), 6) if clean else default


def save_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        print(f"No rows to save for {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"Saved: {path}")


def summarize_by(rows: list[dict], key_field: str) -> list[dict]:
    grouped = defaultdict(list)
    for row in rows:
        grouped[row.get(key_field) or "UNKNOWN"].append(row)

    out = []
    for key, items in grouped.items():
        total = len(items)
        copyable = sum(1 for r in items if r.get("selected_signal_id"))
        no_orderbook = sum(1 for r in items if r.get("latest_status") == "NO_ORDERBOOK")
        policy_blocked = sum(1 for r in items if r.get("latest_status") == "POLICY_BLOCKED")
        skipped_no_position = sum(1 for r in items if r.get("latest_status") == "SKIPPED_NO_POSITION")
        late_copyable = sum(1 for r in items if r.get("latest_status") == "LATE_BUT_COPYABLE")
        fresh_copyable = sum(1 for r in items if r.get("latest_status") == "FRESH_COPYABLE")

        spreads = [_safe_float(r.get("snapshot_spread")) for r in items]
        notionals = [_safe_float(r.get("selected_trade_notional_usd")) for r in items]
        ages = [_safe_float(r.get("selected_trade_age_sec")) for r in items if r.get("selected_signal_id")]

        out.append(
            {
                key_field: key,
                "observations": total,
                "selected_signals": copyable,
                "selection_rate": round(copyable / total, 6) if total else "",
                "fresh_copyable": fresh_copyable,
                "late_copyable": late_copyable,
                "policy_blocked": policy_blocked,
                "skipped_no_position": skipped_no_position,
                "no_orderbook": no_orderbook,
                "median_snapshot_spread": _median(spreads),
                "median_selected_trade_notional_usd": _median(notionals),
                "median_selected_trade_age_sec": _median(ages),
            }
        )

    out.sort(key=lambda r: (r["selected_signals"], r["observations"]), reverse=True)
    return out


def summarize_status(rows: list[dict]) -> list[dict]:
    grouped = defaultdict(int)
    for row in rows:
        grouped[row.get("latest_status") or "UNKNOWN"] += 1

    total = len(rows)
    out = []
    for status, count in sorted(grouped.items(), key=lambda x: x[1], reverse=True):
        out.append(
            {
                "latest_status": status,
                "count": count,
                "share": round(count / total, 6) if total else "",
            }
        )
    return out


def main() -> None:
    rows = list_signal_observations(limit=100000)
    if not rows:
        print("No signal observations yet.")
        return

    by_leader = summarize_by(rows, "leader_user_name")
    by_category = summarize_by(rows, "category")
    by_status = summarize_status(rows)

    print("=== SIGNAL OBSERVATION SUMMARY | BY LEADER ===")
    pprint(by_leader)

    print("\n=== SIGNAL OBSERVATION SUMMARY | BY CATEGORY ===")
    pprint(by_category)

    print("\n=== SIGNAL OBSERVATION SUMMARY | BY STATUS ===")
    pprint(by_status)

    save_csv(by_leader, OUT_LEADER)
    save_csv(by_category, OUT_CATEGORY)
    save_csv(by_status, OUT_STATUS)


if __name__ == "__main__":
    main()
