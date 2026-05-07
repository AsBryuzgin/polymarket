from __future__ import annotations

import argparse
import sys
from pathlib import Path
from pprint import pprint

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.state_migration import (
    apply_legacy_order_attempt_backfill,
    plan_legacy_order_attempt_backfill,
)
from execution.state_store import init_db, list_order_attempts, list_processed_signals


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write backfilled order_attempt rows; default is dry-run",
    )
    args = parser.parse_args()

    init_db()

    if args.apply:
        rows = apply_legacy_order_attempt_backfill()
        print("=== APPLIED LEGACY ORDER ATTEMPT BACKFILL ===")
        pprint(rows)
        print(f"applied={len(rows)}")
        return

    planned = plan_legacy_order_attempt_backfill(
        processed_signal_rows=list_processed_signals(limit=100000),
        order_attempt_rows=list_order_attempts(limit=100000),
    )
    print("=== LEGACY ORDER ATTEMPT BACKFILL DRY-RUN ===")
    pprint([item.__dict__ for item in planned])
    print(f"planned={len(planned)}")


if __name__ == "__main__":
    main()
