from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from pprint import pprint

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.order_recovery import (
    RECOVERY_APPLY_ACK,
    apply_unverified_order_recovery,
    build_unverified_order_recovery_report,
)
from execution.state_store import init_db, list_order_attempts


OUT_FILE = Path("data/recovery_unverified_orders.csv")


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
    parser = argparse.ArgumentParser(
        description="Recover LIVE_SUBMITTED_UNVERIFIED order attempts from exchange order status.",
    )
    parser.add_argument("--apply", action="store_true", help="Apply verified fills to local state.")
    parser.add_argument(
        "--ack",
        default="",
        help=f"Required with --apply: {RECOVERY_APPLY_ACK}",
    )
    parser.add_argument(
        "--out-file",
        default=str(OUT_FILE),
        help="CSV output path for the recovery report.",
    )
    args = parser.parse_args()

    init_db()
    attempts = list_order_attempts(limit=100000)

    if args.apply:
        try:
            rows = apply_unverified_order_recovery(
                order_attempt_rows=attempts,
                apply=True,
                ack=args.ack,
            )
        except ValueError as e:
            print(f"Recovery apply blocked: {e}")
            print(f"Pass --ack {RECOVERY_APPLY_ACK} to apply verified fills.")
            raise SystemExit(2) from e
        title = "=== UNVERIFIED LIVE ORDER RECOVERY APPLY ==="
    else:
        rows = build_unverified_order_recovery_report(
            order_attempt_rows=attempts,
        )
        title = "=== UNVERIFIED LIVE ORDER RECOVERY PREVIEW ==="

    print(title)
    pprint(rows)
    save_csv(rows, Path(args.out_file))


if __name__ == "__main__":
    main()
