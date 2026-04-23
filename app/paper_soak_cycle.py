from __future__ import annotations

import argparse
import sys
from pathlib import Path
from pprint import pprint

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.builder_auth import load_executor_config
from execution.order_router import resolve_execution_mode
from execution.signal_observation_store import init_signal_observation_table
from execution.settlement import run_settlement_cycle
from execution.soak_runner import filter_registry_rows_for_scan, run_soak_cycle, summarize_soak_cycle
from execution.state_store import init_db, list_leader_registry, list_open_positions
from execution.polymarket_executor import fetch_market_snapshot
import execution.state_store as state_store


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one paper-soak scan/process cycle.")
    parser.add_argument(
        "--allow-non-paper",
        action="store_true",
        help="Allow running outside PAPER mode. Intended only for debugging.",
    )
    args = parser.parse_args()

    config = load_executor_config()
    mode = resolve_execution_mode(config)

    if mode != "PAPER" and not args.allow_non_paper:
        raise SystemExit(
            "paper_soak_cycle requires PAPER mode. Use "
            "POLY_EXECUTOR_CONFIG_PATH=config/executor.paper.toml or pass --allow-non-paper."
        )

    init_db()
    init_signal_observation_table()

    registry_rows = list_leader_registry(limit=100000)
    if not registry_rows:
        print("No leader registry rows found. Run the rebalance/lifecycle step first.")
        return
    registry_rows = filter_registry_rows_for_scan(
        registry_rows=registry_rows,
        open_positions=list_open_positions(limit=100000),
    )
    if not registry_rows:
        print("No active leaders or exit-only open positions to scan.")
        return

    rows = run_soak_cycle(registry_rows=registry_rows)
    summary = summarize_soak_cycle(rows)
    settlement_summary = run_settlement_cycle(
        config=config,
        snapshot_loader=fetch_market_snapshot,
    )

    print("=== PAPER SOAK CYCLE ===")
    pprint(
        {
            "mode": mode,
            "state_db_path": str(state_store.DB_PATH),
            "summary": summary,
            "settlement": settlement_summary,
        }
    )
    print("\n=== ROWS ===")
    pprint(rows)


if __name__ == "__main__":
    main()
