from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from execution.allowance import evaluate_live_funding_preflight
from execution.builder_auth import health_snapshot, load_executor_config
from execution.order_router import LIVE_TRADING_ACK, resolve_execution_mode
from execution.reconciliation import reconcile_executor_state
from execution.runtime_guard import evaluate_runtime_guard
from execution.runtime_lock import read_runtime_lock
import execution.state_store as state_store
from execution.state_store import (
    list_open_positions,
    list_order_attempts,
    list_processed_signals,
    list_trade_history,
)
from risk.guards import build_runtime_risk_limits


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


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            try:
                dt = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _row_age_minutes(row: dict[str, Any], *, now: datetime) -> float | None:
    for key in ("updated_at", "created_at", "event_time", "observed_at"):
        dt = _parse_timestamp(row.get(key))
        if dt is not None:
            return max((now - dt).total_seconds() / 60.0, 0.0)
    return None


def _processing_age_counts(
    rows: list[dict[str, Any]],
    *,
    warning_minutes: float,
    critical_minutes: float,
    now: datetime,
) -> tuple[int, int]:
    warning_count = 0
    critical_count = 0
    for row in rows:
        if str(row.get("status") or "") != "PROCESSING":
            continue
        age_minutes = _row_age_minutes(row, now=now)
        if age_minutes is None or age_minutes >= critical_minutes:
            critical_count += 1
        elif age_minutes >= warning_minutes:
            warning_count += 1
    return warning_count, critical_count


def build_executor_health_report(
    *,
    config: dict[str, Any],
    env_health: dict[str, Any],
    open_position_rows: list[dict[str, Any]],
    processed_signal_rows: list[dict[str, Any]],
    order_attempt_rows: list[dict[str, Any]],
    trade_history_rows: list[dict[str, Any]],
    state_db_path: str | None = None,
) -> dict[str, Any]:
    mode = resolve_execution_mode(config)
    global_cfg = config.get("global", {})
    risk_limits = build_runtime_risk_limits(config)
    runtime_guard = evaluate_runtime_guard(
        config=config,
        state_db_path=Path(state_db_path) if state_db_path else state_store.DB_PATH,
    )
    runtime_lock = read_runtime_lock(config)
    alert_cfg = config.get("alerts", {})
    processing_warning_minutes = _safe_float(
        alert_cfg.get("processing_warning_minutes"),
        2.0,
    )
    processing_critical_minutes = _safe_float(
        alert_cfg.get("processing_critical_minutes"),
        10.0,
    )
    now = datetime.now(timezone.utc)

    live_enabled = _bool_or_default(global_cfg.get("live_trading_enabled"), False)
    live_ack = str(global_cfg.get("live_trading_ack", ""))
    trading_disabled = risk_limits.trading_disabled

    processed_status_counts: dict[str, int] = {}
    for row in processed_signal_rows:
        status = str(row.get("status") or "UNKNOWN")
        processed_status_counts[status] = processed_status_counts.get(status, 0) + 1

    attempt_status_counts: dict[str, int] = {}
    for row in order_attempt_rows:
        status = str(row.get("status") or "UNKNOWN")
        attempt_status_counts[status] = attempt_status_counts.get(status, 0) + 1

    reconciliation = reconcile_executor_state(
        trade_history_rows=trade_history_rows,
        open_position_rows=open_position_rows,
        processed_signal_rows=processed_signal_rows,
        order_attempt_rows=order_attempt_rows,
    )

    blockers: list[str] = []
    warnings: list[str] = []

    if trading_disabled:
        blockers.append("risk.trading_disabled is true")

    if not runtime_guard.allowed:
        blockers.append(runtime_guard.reason)

    if mode == "LIVE" and runtime_lock.locked:
        blockers.append(f"runtime lock active: {runtime_lock.reason}")

    if mode == "LIVE":
        if not live_enabled:
            blockers.append("live trading disabled by config")
        if live_ack != LIVE_TRADING_ACK:
            blockers.append("live trading ack is missing or invalid")
        if not env_health.get("env_ok", False):
            blockers.append("executor env is incomplete")
        if risk_limits.capital_base_required and (
            risk_limits.capital_base_usd is None or risk_limits.capital_base_usd <= 0
        ):
            blockers.append("account collateral balance is unavailable or zero")
        elif (
            live_enabled
            and live_ack == LIVE_TRADING_ACK
            and env_health.get("env_ok", False)
            and "funding" in config
            and risk_limits.max_per_trade_usd > 0
        ):
            funding_probe_amount = max(
                risk_limits.min_order_size_usd,
                min(risk_limits.max_per_trade_usd, risk_limits.capital_base_usd or risk_limits.max_per_trade_usd),
            )
            funding_probe = evaluate_live_funding_preflight(
                config=config,
                side="BUY",
                amount_usd=funding_probe_amount,
            )
            if not funding_probe.allowed:
                blockers.append(f"live funding preflight failed: {funding_probe.reason}")

    slow_processing, stuck_processing = _processing_age_counts(
        processed_signal_rows,
        warning_minutes=processing_warning_minutes,
        critical_minutes=processing_critical_minutes,
        now=now,
    )
    if stuck_processing > 0:
        blockers.append(
            f"processed_signals contains {stuck_processing} PROCESSING row(s) older than "
            f"{processing_critical_minutes:g}m"
        )
    elif slow_processing > 0:
        warnings.append(
            f"processed_signals contains {slow_processing} PROCESSING row(s) older than "
            f"{processing_warning_minutes:g}m"
        )
    if reconciliation.summary.get("nonfinal_order_attempts", 0) > 0:
        warnings.append("order_attempts contains non-final rows")
    if reconciliation.summary.get("position_mismatches", 0) > 0:
        warnings.append("trade_history replay does not match copied_positions")
    if reconciliation.summary.get("issues", 0) > 0 and not warnings:
        warnings.append("reconciliation reported issues")

    if mode != "LIVE" and live_enabled:
        warnings.append("live_trading_enabled is true but execution mode is not LIVE")

    health_status = "OK"
    if warnings:
        health_status = "WARN"
    if blockers:
        health_status = "BLOCKED"

    return {
        "health_status": health_status,
        "mode": mode,
        "blockers": blockers,
        "warnings": warnings,
        "global": {
            "simulation": _bool_or_default(global_cfg.get("simulation"), True),
            "preview_mode": _bool_or_default(global_cfg.get("preview_mode"), True),
            "live_trading_enabled": live_enabled,
        },
        "runtime": {
            "allowed": runtime_guard.allowed,
            "reason": runtime_guard.reason,
            "state_db_path": runtime_guard.state_db_path,
            "lock_active": runtime_lock.locked,
            "lock_reason": runtime_lock.reason,
            "lock_path": runtime_lock.path,
        },
        "risk": {
            "trading_disabled": trading_disabled,
            "max_per_trade_usd": risk_limits.max_per_trade_usd,
            "max_position_usd": risk_limits.max_position_usd,
            "max_wallet_exposure_usd": risk_limits.max_wallet_exposure_usd,
            "max_category_exposure_usd": risk_limits.max_category_exposure_usd,
            "max_portfolio_exposure_usd": risk_limits.max_portfolio_exposure_usd,
            "max_daily_realized_loss_usd": risk_limits.max_daily_realized_loss_usd,
            "capital_base_usd": risk_limits.capital_base_usd,
            "capital_base_required": risk_limits.capital_base_required,
            "capital_base_error": risk_limits.capital_base_error,
        },
        "state": {
            "open_positions": len(open_position_rows),
            "processed_signals": len(processed_signal_rows),
            "order_attempts": len(order_attempt_rows),
            "trade_history_events": len(trade_history_rows),
            "processed_status_counts": processed_status_counts,
            "attempt_status_counts": attempt_status_counts,
        },
        "reconciliation": reconciliation.summary,
        "env": env_health,
    }


def executor_health_report() -> dict[str, Any]:
    config = load_executor_config()

    try:
        env_health = health_snapshot()
    except Exception as e:
        env_health = {
            "env_ok": False,
            "health_snapshot_error": str(e),
        }

    return build_executor_health_report(
        config=config,
        env_health=env_health,
        open_position_rows=list_open_positions(limit=100000),
        processed_signal_rows=list_processed_signals(limit=100000),
        order_attempt_rows=list_order_attempts(limit=100000),
        trade_history_rows=list(reversed(list_trade_history(limit=100000))),
        state_db_path=str(state_store.DB_PATH),
    )
