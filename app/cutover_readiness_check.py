from __future__ import annotations

import sys
from dataclasses import asdict
from pathlib import Path
from pprint import pprint
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.builder_auth import health_snapshot, load_executor_config, load_executor_env
from execution.allowance import fetch_collateral_balance_allowance
from execution.cutover_readiness import build_cutover_readiness_report
from execution.positions import fetch_exchange_open_orders, fetch_exchange_positions
from execution.signal_observation_store import (
    init_signal_observation_table,
    list_signal_observations,
)
import execution.state_store as state_store
from execution.state_store import (
    init_db,
    list_open_positions,
    list_order_attempts,
    list_processed_signals,
    list_trade_history,
)


def safe_bool(value: Any, default: bool = False) -> bool:
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


def collect_exchange_snapshots(
    *,
    config: dict[str, Any],
    open_position_rows: list[dict[str, Any]],
) -> tuple[
    list[dict[str, Any]] | None,
    list[dict[str, Any]] | None,
    dict[str, Any] | None,
    list[dict[str, Any]],
]:
    reconciliation_cfg = config.get("reconciliation", {})
    fetch_positions = safe_bool(reconciliation_cfg.get("fetch_exchange_positions"), False)
    fetch_open_orders = safe_bool(reconciliation_cfg.get("fetch_exchange_open_orders"), False)

    exchange_position_rows = None
    exchange_open_order_rows = None
    funding_snapshot = None
    external_issue_rows: list[dict[str, Any]] = []

    if fetch_positions:
        env = load_executor_env()
        if not env.funder_address:
            external_issue_rows.append(
                {
                    "issue_type": "EXCHANGE_FETCH_ERROR",
                    "severity": "WARN",
                    "details": "exchange position fetch skipped: POLY_FUNDER_ADDRESS is empty",
                }
            )
        else:
            try:
                exchange_position_rows = fetch_exchange_positions(env.funder_address)
            except Exception as e:
                external_issue_rows.append(
                    {
                        "issue_type": "EXCHANGE_FETCH_ERROR",
                        "severity": "WARN",
                        "details": f"exchange position fetch failed: {e}",
                    }
                )

    if fetch_open_orders:
        token_ids = sorted({row["token_id"] for row in open_position_rows})
        try:
            exchange_open_order_rows = fetch_exchange_open_orders(token_ids=token_ids)
        except Exception as e:
            external_issue_rows.append(
                {
                    "issue_type": "EXCHANGE_FETCH_ERROR",
                    "severity": "WARN",
                    "details": f"exchange open order fetch failed: {e}",
                }
            )

    funding_cfg = config.get("funding", {})
    if (
        safe_bool(funding_cfg.get("require_positive_balance"), False)
        or funding_cfg.get("min_live_balance_usd") is not None
        or funding_cfg.get("min_live_balance_pct") is not None
        or funding_cfg.get("min_live_allowance_usd") is not None
        or funding_cfg.get("min_live_allowance_pct") is not None
    ):
        try:
            funding_snapshot = asdict(fetch_collateral_balance_allowance(config))
        except Exception as e:
            external_issue_rows.append(
                {
                    "issue_type": "FUNDING_FETCH_ERROR",
                    "severity": "WARN",
                    "details": f"funding snapshot fetch failed: {e}",
                }
            )

    return exchange_position_rows, exchange_open_order_rows, funding_snapshot, external_issue_rows


def main() -> None:
    init_db()
    init_signal_observation_table()

    config = load_executor_config()
    open_position_rows = list_open_positions(limit=100000)
    exchange_position_rows, exchange_open_order_rows, funding_snapshot, external_issue_rows = collect_exchange_snapshots(
        config=config,
        open_position_rows=open_position_rows,
    )

    try:
        env_health = health_snapshot()
    except Exception as e:
        env_health = {
            "env_ok": False,
            "api_creds_ok": False,
            "health_snapshot_error": str(e),
        }

    report = build_cutover_readiness_report(
        config=config,
        env_health=env_health,
        open_position_rows=open_position_rows,
        processed_signal_rows=list_processed_signals(limit=100000),
        order_attempt_rows=list_order_attempts(limit=100000),
        trade_history_rows=list(reversed(list_trade_history(limit=100000))),
        signal_observation_rows=list_signal_observations(limit=100000),
        exchange_position_rows=exchange_position_rows,
        exchange_open_order_rows=exchange_open_order_rows,
        funding_snapshot=funding_snapshot,
        external_issue_rows=external_issue_rows,
        state_db_path=state_store.DB_PATH,
    )

    print("=== CUTOVER READINESS CHECK ===")
    pprint(report)


if __name__ == "__main__":
    main()
