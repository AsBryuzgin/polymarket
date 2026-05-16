from __future__ import annotations

import csv
import sys
from pathlib import Path
from pprint import pprint

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.allocation_runtime import resolve_leader_budget_usd, resolve_total_capital_usd
from execution.builder_auth import load_executor_config
from execution.leader_signal_source import latest_fresh_copyable_signal_from_wallet


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
    config = load_executor_config()
    total_capital_usd = resolve_total_capital_usd(executor_config=config)
    rows = load_allocation(INPUT_FILE)

    print("=== Multi-Leader Fresh Signal Scan ===\n")

    found_any = False

    for idx, row in enumerate(rows, start=1):
        wallet = row["wallet"]
        user_name = row["user_name"]
        category = row["category"]
        leader_budget_usd = resolve_leader_budget_usd(
            row,
            total_capital_usd=total_capital_usd,
        )

        print(f"[{idx}/{len(rows)}] scanning {user_name} | {category} | budget=${leader_budget_usd} | {wallet}")

        try:
            signal, snapshot, summary = latest_fresh_copyable_signal_from_wallet(
                wallet=wallet,
                leader_budget_usd=leader_budget_usd,
            )
        except Exception as e:
            print(f"  error: {e}\n")
            continue

        if signal is None:
            print(
                f"  no signal | latest_side={summary['latest_trade_side']} | "
                f"latest_age={summary['latest_trade_age_sec']} | "
                f"status={summary['latest_status']} | "
                f"reason={summary['latest_reason']}\n"
            )
            continue

        found_any = True
        print("  FOUND SIGNAL:")
        pprint({
            "user_name": user_name,
            "category": category,
            "leader_budget_usd": leader_budget_usd,
            "signal": signal,
            "snapshot": snapshot,
            "summary": summary,
        })
        print()

    if not found_any:
        print("No fresh copyable signals found across all leaders.")


if __name__ == "__main__":
    main()
