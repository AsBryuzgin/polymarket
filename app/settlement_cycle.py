from __future__ import annotations

import argparse
import sys
from pathlib import Path
from pprint import pprint

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.builder_auth import load_executor_config
from execution.polymarket_executor import fetch_market_snapshot
from execution.settlement import run_settlement_cycle
from execution.state_store import init_db


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one settlement/redeem maintenance cycle.")
    args = parser.parse_args()
    del args

    init_db()
    config = load_executor_config()
    report = run_settlement_cycle(
        config=config,
        snapshot_loader=fetch_market_snapshot,
    )
    print("=== SETTLEMENT CYCLE ===")
    pprint(report)


if __name__ == "__main__":
    main()
