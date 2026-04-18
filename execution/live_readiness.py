from __future__ import annotations

from pathlib import Path
from typing import Any

from execution.health_check import build_executor_health_report
from execution.order_recovery import build_unverified_order_recovery_report
from execution.order_router import LIVE_TRADING_ACK, resolve_execution_mode
from execution.reconciliation import reconcile_executor_state
from execution.state_migration import plan_legacy_order_attempt_backfill


def _bool_or_default(value: Any, default: bool) -> bool:
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


def _positive_float_or_none(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _funding_value(snapshot: Any, key: str) -> float | None:
    if snapshot is None:
        return None
    if isinstance(snapshot, dict):
        value = snapshot.get(key)
    else:
        value = getattr(snapshot, key, None)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_live_readiness_report(
    *,
    config: dict[str, Any],
    env_health: dict[str, Any],
    open_position_rows: list[dict[str, Any]],
    processed_signal_rows: list[dict[str, Any]],
    order_attempt_rows: list[dict[str, Any]],
    trade_history_rows: list[dict[str, Any]],
    exchange_position_rows: list[dict[str, Any]] | None = None,
    exchange_open_order_rows: list[dict[str, Any]] | None = None,
    funding_snapshot: Any | None = None,
    external_issue_rows: list[dict[str, Any]] | None = None,
    state_db_path: str | Path | None = None,
) -> dict[str, Any]:
    global_cfg = config.get("global", {})
    funding_cfg = config.get("funding", {})
    live_cfg = config.get("live_execution", {})
    reconciliation_cfg = config.get("reconciliation", {})
    requested_execution_mode = str(global_cfg.get("execution_mode", "")).strip().upper()
    resolved_execution_mode = resolve_execution_mode(config)
    preview_mode = _bool_or_default(global_cfg.get("preview_mode"), True)
    simulation = _bool_or_default(global_cfg.get("simulation"), True)
    live_trading_enabled = _bool_or_default(global_cfg.get("live_trading_enabled"), False)
    live_ack = str(global_cfg.get("live_trading_ack", ""))
    fetch_exchange_positions = _bool_or_default(
        reconciliation_cfg.get("fetch_exchange_positions"),
        False,
    )
    fetch_exchange_open_orders = _bool_or_default(
        reconciliation_cfg.get("fetch_exchange_open_orders"),
        False,
    )
    min_live_balance_usd = _positive_float_or_none(funding_cfg.get("min_live_balance_usd"))
    min_live_balance_pct = _positive_float_or_none(funding_cfg.get("min_live_balance_pct"))
    min_live_allowance_usd = _positive_float_or_none(
        funding_cfg.get("min_live_allowance_usd")
    )
    min_live_allowance_pct = _positive_float_or_none(
        funding_cfg.get("min_live_allowance_pct")
    )
    require_positive_balance = _bool_or_default(
        funding_cfg.get("require_positive_balance"),
        min_live_balance_usd is None and min_live_balance_pct is None,
    )

    funding_balance_usd = _funding_value(funding_snapshot, "balance_usd")
    funding_allowance_usd = _funding_value(funding_snapshot, "allowance_usd")
    if (
        min_live_balance_usd is None
        and min_live_balance_pct is not None
        and funding_balance_usd is not None
    ):
        min_live_balance_usd = round(funding_balance_usd * min_live_balance_pct, 8)
    if (
        min_live_allowance_usd is None
        and min_live_allowance_pct is not None
        and funding_balance_usd is not None
    ):
        min_live_allowance_usd = round(funding_balance_usd * min_live_allowance_pct, 8)

    health = build_executor_health_report(
        config=config,
        env_health=env_health,
        open_position_rows=open_position_rows,
        processed_signal_rows=processed_signal_rows,
        order_attempt_rows=order_attempt_rows,
        trade_history_rows=trade_history_rows,
        state_db_path=str(state_db_path or config.get("state", {}).get("db_path") or ""),
    )

    reconciliation = reconcile_executor_state(
        trade_history_rows=trade_history_rows,
        open_position_rows=open_position_rows,
        processed_signal_rows=processed_signal_rows,
        order_attempt_rows=order_attempt_rows,
        exchange_position_rows=exchange_position_rows,
        exchange_open_order_rows=exchange_open_order_rows,
        external_issue_rows=external_issue_rows,
        exchange_position_qty_tolerance=float(
            reconciliation_cfg.get("position_qty_tolerance", 1e-6)
        ),
    )

    legacy_backfill_plan = plan_legacy_order_attempt_backfill(
        processed_signal_rows=processed_signal_rows,
        order_attempt_rows=order_attempt_rows,
    )
    unverified_orders = build_unverified_order_recovery_report(
        order_attempt_rows=order_attempt_rows,
        order_status_fetcher=lambda _order_id: {},
    )

    blockers: list[str] = []
    warnings: list[str] = []

    if health["health_status"] != "OK":
        blockers.append(f"executor health is {health['health_status']}")

    if not env_health.get("env_ok", False):
        blockers.append("executor env is incomplete")

    if not env_health.get("api_creds_ok", False):
        blockers.append("CLOB API credentials are not verified")

    if not _bool_or_default(funding_cfg.get("require_balance_allowance"), True):
        blockers.append("funding.require_balance_allowance must be true")

    if require_positive_balance:
        if funding_balance_usd is None:
            blockers.append("funding snapshot was not provided")
        elif funding_balance_usd <= 0:
            blockers.append("collateral balance must be positive")

    if min_live_balance_usd is not None:
        if funding_balance_usd is None:
            blockers.append("funding snapshot was not provided")
        elif funding_balance_usd + 1e-9 < min_live_balance_usd:
            blockers.append(
                f"collateral balance {funding_balance_usd:.2f} below "
                f"min_live_balance_usd {min_live_balance_usd:.2f}"
            )

    if min_live_allowance_pct is not None and funding_allowance_usd is None:
        blockers.append("funding allowance snapshot was not provided")

    if min_live_allowance_usd is not None:
        if funding_allowance_usd is None:
            blockers.append("funding allowance snapshot was not provided")
        elif funding_allowance_usd + 1e-9 < min_live_allowance_usd:
            blockers.append(
                f"collateral allowance {funding_allowance_usd:.2f} below "
                f"min_live_allowance_usd {min_live_allowance_usd:.2f}"
            )

    if not _bool_or_default(live_cfg.get("require_verified_fill"), True):
        blockers.append("live_execution.require_verified_fill must be true")

    if requested_execution_mode == "LIVE" and resolved_execution_mode != "LIVE":
        blockers.append(
            f"execution_mode is live but resolved execution mode is {resolved_execution_mode}"
        )

    if live_trading_enabled and live_ack != LIVE_TRADING_ACK:
        blockers.append("live trading ack is missing or invalid")

    if live_trading_enabled and resolved_execution_mode != "LIVE":
        blockers.append("live_trading_enabled is true but resolved execution mode is not LIVE")

    if not fetch_exchange_positions:
        blockers.append("reconciliation.fetch_exchange_positions must be true before live")

    if not fetch_exchange_open_orders:
        blockers.append("reconciliation.fetch_exchange_open_orders must be true before live")

    if fetch_exchange_positions and exchange_position_rows is None:
        blockers.append("exchange position snapshot was not provided")

    if fetch_exchange_open_orders and exchange_open_order_rows is None:
        blockers.append("exchange open-order snapshot was not provided")

    if reconciliation.summary.get("issues", 0) > 0:
        blockers.append("reconciliation has unresolved issues")

    if legacy_backfill_plan:
        blockers.append("legacy order_attempt backfill is still pending")

    if unverified_orders:
        blockers.append("unverified live orders require recovery")

    if open_position_rows and resolved_execution_mode != "LIVE":
        blockers.append(
            f"{resolved_execution_mode.lower()} runtime DB contains open positions; "
            "use a clean live DB or reconcile first"
        )

    if resolved_execution_mode != "LIVE":
        warnings.append(
            f"current config resolves to {resolved_execution_mode}; this is a pre-switch readiness check"
        )

    if not live_trading_enabled:
        warnings.append("live_trading_enabled is false")
    else:
        warnings.append("live_trading_enabled is already true")

    status = "GO" if not blockers else "NO_GO"

    return {
        "readiness_status": status,
        "blockers": blockers,
        "warnings": warnings,
        "mode": {
            "requested_execution_mode": requested_execution_mode or "",
            "resolved_execution_mode": resolved_execution_mode,
            "simulation": simulation,
            "preview_mode": preview_mode,
            "live_trading_enabled": live_trading_enabled,
            "live_ack_valid": live_ack == LIVE_TRADING_ACK,
        },
        "health": health,
        "reconciliation": reconciliation.summary,
        "funding": {
            "snapshot_provided": funding_snapshot is not None,
            "balance_usd": funding_balance_usd,
            "allowance_usd": funding_allowance_usd,
            "min_live_balance_usd": min_live_balance_usd,
            "min_live_allowance_usd": min_live_allowance_usd,
            "min_live_balance_pct": min_live_balance_pct,
            "min_live_allowance_pct": min_live_allowance_pct,
            "require_positive_balance": require_positive_balance,
        },
        "legacy_backfill_pending": len(legacy_backfill_plan),
        "unverified_live_orders": len(unverified_orders),
    }
