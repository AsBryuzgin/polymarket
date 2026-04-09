from __future__ import annotations

from pathlib import Path

from app.build_live_universe_stable import main as build_stable_live_universe
from app.apply_rebalance_lifecycle import main as apply_rebalance_lifecycle


LIVE_FILE = Path("data/shortlists/live_portfolio_allocation.csv")
REPORT_FILE = Path("data/shortlists/live_rebalance_report.csv")
STATE_FILE = Path("data/rebalance_state.json")


def main() -> None:
    print("=== STEP 1/2: BUILD STABLE LIVE UNIVERSE ===")
    build_stable_live_universe()

    if not LIVE_FILE.exists():
        raise FileNotFoundError(f"Expected live file was not created: {LIVE_FILE}")
    if not REPORT_FILE.exists():
        raise FileNotFoundError(f"Expected rebalance report was not created: {REPORT_FILE}")
    if not STATE_FILE.exists():
        raise FileNotFoundError(f"Expected rebalance state was not created: {STATE_FILE}")

    print("\n=== STEP 2/2: APPLY REBALANCE LIFECYCLE ===")
    apply_rebalance_lifecycle()

    print("\n=== REBALANCE CYCLE COMPLETE ===")
    print(f"Live universe: {LIVE_FILE}")
    print(f"Rebalance report: {REPORT_FILE}")
    print(f"Rebalance state: {STATE_FILE}")


if __name__ == "__main__":
    main()
