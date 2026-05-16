from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from pprint import pprint
from dataclasses import asdict

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.alerts import build_executor_alerts, has_critical_alerts
from execution.alert_delivery import deliver_alerts
from execution.builder_auth import load_executor_config
from execution.health_check import executor_health_report
from execution.runtime_lock import activate_runtime_lock, runtime_lock_activate_on_critical
from execution.state_store import (
    init_db,
    list_order_attempts,
    list_processed_signals,
)


OUT_FILE = Path("data/executor_alerts_latest.json")


def save_json(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2, default=str)
    print(f"Saved: {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build executor safety alerts.")
    parser.add_argument(
        "--out-file",
        default=str(OUT_FILE),
        help="JSON output path for the latest alert snapshot.",
    )
    parser.add_argument(
        "--deliver",
        action="store_true",
        help="Send alerts to configured external destinations.",
    )
    args = parser.parse_args()

    init_db()
    config = load_executor_config()

    try:
        health = executor_health_report()
    except Exception as e:
        health = {
            "health_status": "BLOCKED",
            "blockers": [f"health report failed: {e}"],
            "warnings": [],
        }

    alerts = build_executor_alerts(
        config=config,
        processed_signal_rows=list_processed_signals(limit=100000),
        order_attempt_rows=list_order_attempts(limit=100000),
        health_report=health,
    )

    print("=== EXECUTOR ALERTS ===")
    pprint(alerts)
    print(f"critical={has_critical_alerts(alerts)} | count={len(alerts)}")
    save_json(alerts, Path(args.out_file))

    if has_critical_alerts(alerts) and runtime_lock_activate_on_critical(config):
        lock = activate_runtime_lock(
            config,
            reason=f"critical executor alerts: {len(alerts)} alert(s)",
            source="executor_alerts",
            alerts=alerts,
        )
        print("=== RUNTIME LOCK ACTIVATED ===")
        pprint(asdict(lock))

    if args.deliver:
        delivery_results = deliver_alerts(config=config, alerts=alerts)
        print("=== ALERT DELIVERY ===")
        pprint([asdict(row) for row in delivery_results])

    if has_critical_alerts(alerts):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
