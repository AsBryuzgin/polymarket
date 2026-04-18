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
from execution.order_router import resolve_execution_mode
from execution.signal_observation_store import init_signal_observation_table
from execution.soak_runner import run_soak_cycle, summarize_soak_cycle
from execution.state_store import init_db, list_leader_registry
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
            registry_rows = list_leader_registry(limit=100000)
            if not registry_rows:
                print("No leader registry rows found. Run the rebalance/lifecycle step first.")
                return

            started = time.strftime("%Y-%m-%d %H:%M:%S")
            rows = run_soak_cycle(registry_rows=registry_rows)
            summary = summarize_soak_cycle(rows)

            print(f"\n--- paper soak cycle {cycle} at {started} ---")
            pprint(summary)

            if args.max_cycles and cycle >= args.max_cycles:
                print("Reached max_cycles, stopping.")
                return

            time.sleep(interval_sec)
    except KeyboardInterrupt:
        print("\nStopped by user.")


if __name__ == "__main__":
    main()
