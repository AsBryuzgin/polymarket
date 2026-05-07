from __future__ import annotations

import csv
import sys
from pathlib import Path
from pprint import pprint

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.builder_auth import load_executor_config, load_executor_env
from execution.positions import fetch_exchange_open_orders, fetch_exchange_positions
from execution.reconciliation import reconcile_executor_state
from execution.state_store import (
    init_db,
    list_open_positions,
    list_order_attempts,
    list_processed_signals,
    list_trade_history,
)


OUT_POSITIONS = Path("data/reconciliation_positions.csv")
OUT_ISSUES = Path("data/reconciliation_issues.csv")


def safe_bool(value, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return bool(value)


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
    init_db()

    config = load_executor_config()
    reconciliation_cfg = config.get("reconciliation", {})
    fetch_positions = safe_bool(reconciliation_cfg.get("fetch_exchange_positions"), False)
    fetch_open_orders = safe_bool(reconciliation_cfg.get("fetch_exchange_open_orders"), False)
    qty_tolerance = float(reconciliation_cfg.get("position_qty_tolerance", 1e-6))

    exchange_positions = None
    exchange_open_orders = []
    external_issues = []

    if fetch_positions:
        env = load_executor_env()
        if not env.funder_address:
            external_issues.append(
                {
                    "issue_type": "EXCHANGE_FETCH_ERROR",
                    "severity": "WARN",
                    "details": "exchange position fetch skipped: POLY_FUNDER_ADDRESS is empty",
                }
            )
        else:
            try:
                exchange_positions = fetch_exchange_positions(env.funder_address)
            except Exception as e:
                external_issues.append(
                    {
                        "issue_type": "EXCHANGE_FETCH_ERROR",
                        "severity": "WARN",
                        "details": f"exchange position fetch failed: {e}",
                    }
                )

    if fetch_open_orders:
        token_ids = sorted({row["token_id"] for row in list_open_positions(limit=100000)})
        try:
            exchange_open_orders = fetch_exchange_open_orders(token_ids=token_ids)
        except Exception as e:
            external_issues.append(
                {
                    "issue_type": "EXCHANGE_FETCH_ERROR",
                    "severity": "WARN",
                    "details": f"exchange open order fetch failed: {e}",
                }
            )

    report = reconcile_executor_state(
        trade_history_rows=list(reversed(list_trade_history(limit=100000))),
        open_position_rows=list_open_positions(limit=100000),
        processed_signal_rows=list_processed_signals(limit=100000),
        order_attempt_rows=list_order_attempts(limit=100000),
        exchange_position_rows=exchange_positions,
        exchange_open_order_rows=exchange_open_orders,
        external_issue_rows=external_issues,
        exchange_position_qty_tolerance=qty_tolerance,
    )

    print("=== RECONCILIATION SUMMARY ===")
    pprint(report.summary)

    print("\n=== RECONCILIATION ISSUES ===")
    pprint(report.issue_rows)

    save_csv(report.position_rows, OUT_POSITIONS)
    save_csv(report.issue_rows, OUT_ISSUES)


if __name__ == "__main__":
    main()
