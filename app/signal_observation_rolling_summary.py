from __future__ import annotations

import csv
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from pprint import pprint

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import execution.state_store as state_store


OUT_OVERVIEW = Path("data/signal_observation_rolling_overview.csv")
OUT_LEADER = Path("data/signal_observation_rolling_by_leader.csv")
OUT_CATEGORY = Path("data/signal_observation_rolling_by_category.csv")
OUT_STATUS = Path("data/signal_observation_rolling_by_status.csv")


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(state_store.DB_PATH)
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


def _parse_observed_at(x: str | None):
    if not x:
        return None
    try:
        return datetime.fromisoformat(str(x).replace("Z", "+00:00").replace(" ", "T")).astimezone(timezone.utc)
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
        selected = sum(1 for r in items if r.get("selected_signal_id"))
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
                "selected_signals": selected,
                "selection_rate": round(selected / total, 6) if total else "",
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


def build_overview(rows: list[dict], hours: int) -> list[dict]:
    total = len(rows)
    selected = sum(1 for r in rows if r.get("selected_signal_id"))
    no_orderbook = sum(1 for r in rows if r.get("latest_status") == "NO_ORDERBOOK")
    policy_blocked = sum(1 for r in rows if r.get("latest_status") == "POLICY_BLOCKED")
    skipped_no_position = sum(1 for r in rows if r.get("latest_status") == "SKIPPED_NO_POSITION")
    late_copyable = sum(1 for r in rows if r.get("latest_status") == "LATE_BUT_COPYABLE")
    fresh_copyable = sum(1 for r in rows if r.get("latest_status") == "FRESH_COPYABLE")

    spreads = [_safe_float(r.get("snapshot_spread")) for r in rows]
    notionals = [_safe_float(r.get("selected_trade_notional_usd")) for r in rows]
    ages = [_safe_float(r.get("selected_trade_age_sec")) for r in rows if r.get("selected_signal_id")]

    return [{
        "window_hours": hours,
        "observations": total,
        "selected_signals": selected,
        "selection_rate": round(selected / total, 6) if total else "",
        "fresh_copyable": fresh_copyable,
        "late_copyable": late_copyable,
        "policy_blocked": policy_blocked,
        "skipped_no_position": skipped_no_position,
        "no_orderbook": no_orderbook,
        "median_snapshot_spread": _median(spreads),
        "median_selected_trade_notional_usd": _median(notionals),
        "median_selected_trade_age_sec": _median(ages),
    }]


def main() -> None:
    hours = int(sys.argv[1]) if len(sys.argv) > 1 else 24

    rows = list_signal_observations(limit=100000)
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=hours)

    filtered = []
    for row in rows:
        dt = _parse_observed_at(row.get("observed_at"))
        if dt is None:
            continue
        if dt >= cutoff:
            filtered.append(row)

    if not filtered:
        print(f"No signal observations in the last {hours} hours.")
        return

    overview = build_overview(filtered, hours)
    by_leader = summarize_by(filtered, "leader_user_name")
    by_category = summarize_by(filtered, "category")
    by_status = summarize_status(filtered)

    print(f"=== SIGNAL OBSERVATION ROLLING SUMMARY | LAST {hours} HOURS ===")
    pprint(overview)

    print("\n=== BY LEADER ===")
    pprint(by_leader)

    print("\n=== BY CATEGORY ===")
    pprint(by_category)

    print("\n=== BY STATUS ===")
    pprint(by_status)

    save_csv(overview, OUT_OVERVIEW)
    save_csv(by_leader, OUT_LEADER)
    save_csv(by_category, OUT_CATEGORY)
    save_csv(by_status, OUT_STATUS)


if __name__ == "__main__":
    main()
