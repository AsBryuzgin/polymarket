from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from execution.alerts import build_executor_alerts, has_critical_alerts
from execution.order_router import resolve_execution_mode
from execution.runtime_lock import (
    activate_runtime_lock,
    read_runtime_lock,
    runtime_lock_activate_on_critical,
)
from execution.state_store import list_order_attempts, list_processed_signals


@dataclass(frozen=True)
class LiveSafetyDecision:
    allowed: bool
    reason: str
    mode: str
    critical_alerts: int = 0
    alerts: list[dict[str, Any]] = field(default_factory=list)


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


def evaluate_live_buy_safety(
    *,
    config: dict[str, Any],
    processed_signal_rows: list[dict[str, Any]] | None = None,
    order_attempt_rows: list[dict[str, Any]] | None = None,
) -> LiveSafetyDecision:
    mode = resolve_execution_mode(config)
    safety_cfg = config.get("live_safety", {})
    enabled = _bool_or_default(safety_cfg.get("enable_stop_buy_on_critical"), True)

    if mode != "LIVE":
        return LiveSafetyDecision(
            allowed=True,
            reason="live buy safety gate applies only in LIVE mode",
            mode=mode,
        )

    if not enabled:
        return LiveSafetyDecision(
            allowed=True,
            reason="live buy safety gate disabled by config",
            mode=mode,
        )

    lock_state = read_runtime_lock(config)
    if lock_state.locked:
        return LiveSafetyDecision(
            allowed=False,
            reason=f"runtime lock active: {lock_state.reason}",
            mode=mode,
            critical_alerts=1,
            alerts=[
                {
                    "severity": "CRITICAL",
                    "alert_type": "RUNTIME_LOCK_ACTIVE",
                    "message": lock_state.reason,
                    "details": lock_state.payload,
                }
            ],
        )

    if processed_signal_rows is None:
        processed_signal_rows = list_processed_signals(limit=100000)
    if order_attempt_rows is None:
        order_attempt_rows = list_order_attempts(limit=100000)

    alerts = build_executor_alerts(
        config=config,
        processed_signal_rows=processed_signal_rows,
        order_attempt_rows=order_attempt_rows,
    )
    critical_count = sum(1 for row in alerts if row.get("severity") == "CRITICAL")

    if has_critical_alerts(alerts):
        first = next(row for row in alerts if row.get("severity") == "CRITICAL")
        if runtime_lock_activate_on_critical(config):
            activate_runtime_lock(
                config,
                reason=f"critical alert: {first.get('message')}",
                source="live_safety",
                alerts=alerts,
            )
        return LiveSafetyDecision(
            allowed=False,
            reason=f"live stop-buy active: {first.get('message')}",
            mode=mode,
            critical_alerts=critical_count,
            alerts=alerts,
        )

    return LiveSafetyDecision(
        allowed=True,
        reason="ok",
        mode=mode,
        alerts=alerts,
    )
