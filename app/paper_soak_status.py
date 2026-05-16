from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from pprint import pprint

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.builder_auth import load_executor_config
from execution.signal_observation_store import init_signal_observation_table, list_signal_observations
from execution.soak_status import (
    build_paper_soak_status_report,
    flatten_paper_soak_status_report,
)
from execution.state_store import (
    init_db,
    list_leader_registry,
    list_open_positions,
    list_order_attempts,
    list_processed_signals,
    list_trade_history,
)
import execution.state_store as state_store


DEFAULT_JSON_OUT = Path("data/paper_soak_status_latest.json")
DEFAULT_CSV_OUT = Path("data/paper_soak_status_history.csv")


def append_csv_row(row: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    existing_fields: list[str] = []

    if exists:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            try:
                existing_fields = next(reader)
            except StopIteration:
                existing_fields = []

    fieldnames = list(existing_fields)
    for key in row:
        if key not in fieldnames:
            fieldnames.append(key)

    rewrite_existing = bool(existing_fields and fieldnames != existing_fields)
    existing_rows: list[dict] = []
    if rewrite_existing:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            existing_rows = list(reader)

    mode = "w" if rewrite_existing or not exists else "a"
    with path.open(mode, encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if mode == "w":
            writer.writeheader()
            for existing_row in existing_rows:
                writer.writerow(existing_row)
        writer.writerow(row)


def save_snapshot(report: dict, *, json_path: Path, csv_path: Path) -> None:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False, default=str)

    row = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        **flatten_paper_soak_status_report(report),
    }
    append_csv_row(row, csv_path)

    print(f"Saved JSON: {json_path}")
    print(f"Appended CSV: {csv_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Show paper-soak progress from local SQLite state.")
    parser.add_argument(
        "--save",
        action="store_true",
        help="Save latest JSON and append one compact row to the history CSV.",
    )
    parser.add_argument(
        "--json-out",
        type=Path,
        default=DEFAULT_JSON_OUT,
        help=f"JSON output path for --save. Default: {DEFAULT_JSON_OUT}",
    )
    parser.add_argument(
        "--csv-out",
        type=Path,
        default=DEFAULT_CSV_OUT,
        help=f"CSV history path for --save. Default: {DEFAULT_CSV_OUT}",
    )
    args = parser.parse_args()

    init_db()
    init_signal_observation_table()

    config = load_executor_config()
    report = build_paper_soak_status_report(
        config=config,
        leader_registry_rows=list_leader_registry(limit=100000),
        open_position_rows=list_open_positions(limit=100000),
        processed_signal_rows=list_processed_signals(limit=100000),
        order_attempt_rows=list_order_attempts(limit=100000),
        trade_history_rows=list(reversed(list_trade_history(limit=100000))),
        signal_observation_rows=list_signal_observations(limit=100000),
        state_db_path=state_store.DB_PATH,
    )

    print("=== PAPER SOAK STATUS ===")
    pprint(report)

    if args.save:
        save_snapshot(report, json_path=args.json_out, csv_path=args.csv_out)


if __name__ == "__main__":
    main()
