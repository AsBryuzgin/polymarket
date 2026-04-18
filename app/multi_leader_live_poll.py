from __future__ import annotations

import csv
import sys
import time
from pathlib import Path
from pprint import pprint

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.allocation_runtime import resolve_leader_budget_usd, resolve_total_capital_usd
from execution.builder_auth import load_executor_config
from execution.copy_worker import process_signal
from execution.leader_signal_source import latest_fresh_copyable_signal_from_wallet
from execution.state_backup import backup_state_db
from execution.state_store import init_db


INPUT_FILE = Path("data/shortlists/live_portfolio_allocation.csv")


def load_allocation(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Missing allocation file: {path}")

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    for row in rows:
        row["weight"] = float(row["weight"])
        row["final_wss"] = float(row["final_wss"])
        row["leaderboard_pnl"] = float(row["leaderboard_pnl"])
        row["leaderboard_volume"] = float(row["leaderboard_volume"])

    return rows


def main() -> None:
    init_db()
    config = load_executor_config()
    poll_interval_sec = int(config.get("global", {}).get("poll_interval_sec", 2))
    total_capital_usd = resolve_total_capital_usd(executor_config=config)

    rows = load_allocation(INPUT_FILE)

    print("=== Multi-Leader Live Poll ===")
    print(f"leaders={len(rows)} | poll_interval_sec={poll_interval_sec}")
    pprint({"startup_backup": backup_state_db(config=config, label="startup")})
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            cycle_started = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n--- cycle started at {cycle_started} ---")

            for idx, row in enumerate(rows, start=1):
                wallet = row["wallet"]
                user_name = row["user_name"]
                category = row["category"]
                leader_budget_usd = resolve_leader_budget_usd(
                    row,
                    total_capital_usd=total_capital_usd,
                )

                print(f"[{idx}/{len(rows)}] {user_name} | {category} | budget=${leader_budget_usd} | {wallet}")

                try:
                    signal, snapshot, summary = latest_fresh_copyable_signal_from_wallet(
                        wallet=wallet,
                        leader_budget_usd=leader_budget_usd,
                    )
                except Exception as e:
                    print(f"  source_error: {e}")
                    continue

                if signal is None:
                    print(
                        f"  no signal | latest_side={summary['latest_trade_side']} | "
                        f"latest_age={summary['latest_trade_age_sec']} | "
                        f"reason={summary['latest_reason']}"
                    )
                    continue

                print("  SIGNAL FOUND")
                pprint({
                    "signal": signal,
                    "snapshot": snapshot,
                    "summary": summary,
                })

                try:
                    result = process_signal(signal)
                    print("  PROCESS RESULT")
                    pprint(result)
                except Exception as e:
                    print(f"  process_error: {e}")

            pprint({"cycle_backup": backup_state_db(config=config, label="after_cycle")})
            time.sleep(poll_interval_sec)

    except KeyboardInterrupt:
        print("\nStopped by user.")


if __name__ == "__main__":
    main()
