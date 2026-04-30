from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


CRITICAL_ORDER_ATTEMPT_STATUSES = {
    "LIVE_SUBMITTED_UNVERIFIED",
    "LIVE_SUBMIT_ERROR",
    "EXECUTION_ERROR",
}

WARNING_ORDER_ATTEMPT_STATUSES = {
    "LIVE_REJECTED",
    "LIVE_PREFLIGHT_BLOCKED",
}

CRITICAL_SIGNAL_STATUSES = {
    "EXECUTION_UNKNOWN",
    "EXECUTION_ERROR",
}


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


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


def _age_minutes(row: dict[str, Any], *, now: datetime) -> float | None:
    for key in ("updated_at", "created_at", "event_time", "observed_at"):
        dt = _parse_timestamp(row.get(key))
        if dt is not None:
            return max((now - dt).total_seconds() / 60.0, 0.0)
    return None


def _add_alert(
    alerts: list[dict[str, Any]],
    *,
    severity: str,
    alert_type: str,
    message: str,
    row: dict[str, Any] | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    payload: dict[str, Any] = {
        "severity": severity,
        "alert_type": alert_type,
        "message": message,
    }
    if row:
        for key in ("signal_id", "attempt_id", "leader_wallet", "token_id", "side", "status"):
            if key in row:
                payload[key] = row.get(key)
    if details:
        payload["details"] = details
    alerts.append(payload)


def build_executor_alerts(
    *,
    config: dict[str, Any],
    processed_signal_rows: list[dict[str, Any]],
    order_attempt_rows: list[dict[str, Any]],
    health_report: dict[str, Any] | None = None,
    live_readiness_report: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    alert_cfg = config.get("alerts", {})
    include_warnings = _bool_or_default(alert_cfg.get("include_warnings"), True)
    processing_warning_minutes = _safe_float(
        alert_cfg.get("processing_warning_minutes"),
        2.0,
    )
    processing_critical_minutes = _safe_float(
        alert_cfg.get("processing_critical_minutes"),
        10.0,
    )
    now = now.astimezone(timezone.utc) if now is not None else datetime.now(timezone.utc)

    alerts: list[dict[str, Any]] = []

    if health_report:
        health_status = str(health_report.get("health_status") or "")
        if health_status == "BLOCKED":
            for blocker in health_report.get("blockers", []):
                _add_alert(
                    alerts,
                    severity="CRITICAL",
                    alert_type="EXECUTOR_HEALTH_BLOCKED",
                    message=str(blocker),
                )
        elif health_status == "WARN" and include_warnings:
            for warning in health_report.get("warnings", []):
                _add_alert(
                    alerts,
                    severity="WARNING",
                    alert_type="EXECUTOR_HEALTH_WARNING",
                    message=str(warning),
                )

    if live_readiness_report and live_readiness_report.get("readiness_status") == "NO_GO":
        for blocker in live_readiness_report.get("blockers", []):
            _add_alert(
                alerts,
                severity="CRITICAL",
                alert_type="LIVE_READINESS_BLOCKED",
                message=str(blocker),
            )

    for row in order_attempt_rows:
        status = str(row.get("status") or "")
        if status in CRITICAL_ORDER_ATTEMPT_STATUSES:
            _add_alert(
                alerts,
                severity="CRITICAL",
                alert_type=f"ORDER_{status}",
                message=f"order attempt requires review: status={status}",
                row=row,
            )
        elif include_warnings and status in WARNING_ORDER_ATTEMPT_STATUSES:
            _add_alert(
                alerts,
                severity="WARNING",
                alert_type=f"ORDER_{status}",
                message=f"order attempt should be reviewed: status={status}",
                row=row,
            )

    for row in processed_signal_rows:
        status = str(row.get("status") or "")
        if status in CRITICAL_SIGNAL_STATUSES:
            _add_alert(
                alerts,
                severity="CRITICAL",
                alert_type=f"SIGNAL_{status}",
                message=f"processed signal requires review: status={status}",
                row=row,
            )
            continue

        if status == "PROCESSING":
            age = _age_minutes(row, now=now)
            if age is None or age >= processing_critical_minutes:
                _add_alert(
                    alerts,
                    severity="CRITICAL",
                    alert_type="SIGNAL_STUCK_PROCESSING",
                    message="processed signal is stuck in PROCESSING",
                    row=row,
                    details={"age_minutes": age},
                )
            elif include_warnings and age >= processing_warning_minutes:
                _add_alert(
                    alerts,
                    severity="WARNING",
                    alert_type="SIGNAL_SLOW_PROCESSING",
                    message="processed signal has been PROCESSING longer than expected",
                    row=row,
                    details={"age_minutes": round(age, 4)},
                )

    severity_rank = {"CRITICAL": 0, "WARNING": 1, "INFO": 2}
    return sorted(
        alerts,
        key=lambda row: (
            severity_rank.get(str(row.get("severity")), 99),
            str(row.get("alert_type") or ""),
            str(row.get("signal_id") or ""),
        ),
    )


def has_critical_alerts(alerts: list[dict[str, Any]]) -> bool:
    return any(row.get("severity") == "CRITICAL" for row in alerts)
