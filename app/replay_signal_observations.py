from __future__ import annotations

import csv
import sqlite3
import sys
from pathlib import Path
from pprint import pprint

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtest.replay import replay_signal_observations as run_signal_observation_replay
import execution.state_store as state_store
from execution.builder_auth import load_executor_config


OUT_EVENTS = Path("data/replay_signal_observations_events.csv")
OUT_SUMMARY_LEADER = Path("data/replay_signal_observations_by_leader.csv")
OUT_SUMMARY_CATEGORY = Path("data/replay_signal_observations_by_category.csv")


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
        ORDER BY observation_id ASC
        LIMIT ?
        """,
        (limit,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


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


def main() -> None:
    cfg = load_executor_config()
    risk = cfg.get("risk", {})
    sizing = cfg.get("sizing", {})

    min_order_size_usd = float(risk.get("min_order_size_usd", 1.0))
    max_per_trade_usd = float(risk.get("max_per_trade_usd", 2.0))
    leader_trade_notional_copy_fraction = float(
        sizing.get("leader_trade_notional_copy_fraction", 0.20)
    )

    observations = list_signal_observations(limit=100000)
    if not observations:
        print("No signal observations yet.")
        return

    report = run_signal_observation_replay(
        observations,
        leader_trade_notional_copy_fraction=leader_trade_notional_copy_fraction,
        min_order_size_usd=min_order_size_usd,
        max_per_trade_usd=max_per_trade_usd,
    )
    replay_events = report.event_rows
    skipped_rows = report.skipped_rows
    by_leader = report.by_leader
    by_category = report.by_category

    print("=== REPLAY SIGNAL OBSERVATIONS | EVENTS ===")
    pprint(replay_events[:20])

    print("\n=== REPLAY SIGNAL OBSERVATIONS | BY LEADER ===")
    pprint(by_leader)

    print("\n=== REPLAY SIGNAL OBSERVATIONS | BY CATEGORY ===")
    pprint(by_category)

    print("\n=== REPLAY SIGNAL OBSERVATIONS | SKIPPED ===")
    pprint(skipped_rows[:50])

    print("\n=== REPLAY SIGNAL OBSERVATIONS | METRICS ===")
    pprint(report.metrics)

    save_csv(replay_events, OUT_EVENTS)
    save_csv(by_leader, OUT_SUMMARY_LEADER)
    save_csv(by_category, OUT_SUMMARY_CATEGORY)


if __name__ == "__main__":
    main()
