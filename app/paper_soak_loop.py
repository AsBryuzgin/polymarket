from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from pprint import pprint

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.builder_auth import load_executor_config
from execution.copy_worker import flush_signal_batches
from execution.order_router import resolve_execution_mode
from execution.polling import remaining_cycle_sleep_sec, sleep_until_next_cycle
from execution.signal_observation_store import init_signal_observation_table
from execution.settlement import run_settlement_cycle
from execution.soak_runner import filter_registry_rows_for_scan, run_soak_cycle, summarize_soak_cycle
from execution.state_store import (
    init_db,
    list_leader_registry,
    list_open_positions,
    mark_stale_processing_signals,
)
from execution.polymarket_executor import fetch_market_snapshot
import execution.state_store as state_store


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a repeated paper-soak loop.")
    parser.add_argument(
        "--interval-sec",
        type=float,
        default=None,
        help="Seconds between cycles. Defaults to global.poll_interval_sec.",
    )
    parser.add_argument(
        "--max-cycles",
        type=int,
        default=0,
        help="Stop after N cycles. Default 0 means infinite.",
    )
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
            "paper_soak_loop requires PAPER mode. Use "
            "POLY_EXECUTOR_CONFIG_PATH=config/executor.paper.toml or pass --allow-non-paper."
        )

    interval_sec = args.interval_sec
    if interval_sec is None:
        interval_sec = float(config.get("global", {}).get("poll_interval_sec", 2))
    soak_cfg = config.get("paper_soak", {})
    max_fetch_workers = int(soak_cfg.get("max_fetch_workers", 1))
    max_process_workers = int(soak_cfg.get("max_process_workers", 1))
    stale_processing_max_age_sec = float(
        soak_cfg.get("stale_processing_max_age_sec", 900.0)
    )

    init_db()
    init_signal_observation_table()

    print("=== PAPER SOAK LOOP ===")
    pprint(
        {
            "mode": mode,
            "state_db_path": str(state_store.DB_PATH),
            "interval_sec": interval_sec,
            "max_cycles": args.max_cycles or "infinite",
        }
    )

    cycle = 0
    try:
        while True:
            cycle += 1
            cycle_started_monotonic = time.monotonic()
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

            started = time.strftime("%Y-%m-%d %H:%M:%S")
            rows = run_soak_cycle(
                registry_rows=registry_rows,
                batch_flusher=lambda: flush_signal_batches(config),
                max_fetch_workers=max_fetch_workers,
                max_process_workers=max_process_workers,
            )
            stale_processing_summary = mark_stale_processing_signals(
                max_age_sec=stale_processing_max_age_sec,
            )
            summary = summarize_soak_cycle(rows)
            settlement_summary = run_settlement_cycle(
                config=config,
                snapshot_loader=fetch_market_snapshot,
            )
            cycle_elapsed_sec = time.monotonic() - cycle_started_monotonic
            sleep_sec = remaining_cycle_sleep_sec(
                cycle_started_monotonic=cycle_started_monotonic,
                interval_sec=interval_sec,
            )
            summary["cycle_elapsed_sec"] = round(cycle_elapsed_sec, 3)
            summary["next_sleep_sec"] = round(sleep_sec, 3)
            summary["max_fetch_workers"] = max_fetch_workers
            summary["max_process_workers"] = max_process_workers
            summary["stale_processing_recovery"] = stale_processing_summary

            print(f"\n--- paper soak cycle {cycle} at {started} ---")
            pprint(summary)
            print("--- settlement ---")
            pprint(settlement_summary)

            if args.max_cycles and cycle >= args.max_cycles:
                print("Reached max_cycles, stopping.")
                return

            sleep_until_next_cycle(
                cycle_started_monotonic=cycle_started_monotonic,
                interval_sec=interval_sec,
            )
    except KeyboardInterrupt:
        print("\nStopped by user.")


if __name__ == "__main__":
    main()
