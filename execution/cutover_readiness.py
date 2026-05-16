from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from execution.live_readiness import build_live_readiness_report
from execution.order_router import resolve_execution_mode
from execution.state_store import DEFAULT_DB_PATH


ERROR_ATTEMPT_STATUSES = {
    "EXECUTION_ERROR",
    "LIVE_SUBMIT_ERROR",
    "LIVE_REJECTED",
}

UNKNOWN_ATTEMPT_STATUSES = {
    "LIVE_SUBMITTED_UNVERIFIED",
}

ERROR_SIGNAL_STATUSES = {
    "EXECUTION_ERROR",
    "EXECUTION_UNKNOWN",
}


@dataclass(frozen=True)
class SoakWindow:
    started_at: str | None
    ended_at: str | None
    hours: float
    max_gap_minutes: float
    event_count: int


@dataclass(frozen=True)
class CutoverReadinessReport:
    cutover_status: str
    blockers: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    soak: dict[str, Any] = field(default_factory=dict)
    counts: dict[str, Any] = field(default_factory=dict)
    live_readiness: dict[str, Any] = field(default_factory=dict)


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


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


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


def _row_timestamp(row: dict[str, Any]) -> datetime | None:
    for key in ("created_at", "updated_at", "event_time", "observed_at"):
        dt = _parse_timestamp(row.get(key))
        if dt is not None:
            return dt
    return None


def _build_soak_window(*row_sets: list[dict[str, Any]]) -> SoakWindow:
    timestamps: list[datetime] = []
    for rows in row_sets:
        for row in rows:
            dt = _row_timestamp(row)
            if dt is not None:
                timestamps.append(dt)

    if not timestamps:
        return SoakWindow(None, None, 0.0, 0.0, 0)

    timestamps.sort()
    started = min(timestamps)
    ended = max(timestamps)
    hours = max((ended - started).total_seconds() / 3600.0, 0.0)
    max_gap_minutes = 0.0
    for earlier, later in zip(timestamps, timestamps[1:]):
        max_gap_minutes = max(max_gap_minutes, (later - earlier).total_seconds() / 60.0)

    return SoakWindow(
        started_at=started.isoformat(),
        ended_at=ended.isoformat(),
        hours=round(hours, 4),
        max_gap_minutes=round(max_gap_minutes, 4),
        event_count=len(timestamps),
    )


def build_cutover_readiness_report(
    *,
    config: dict[str, Any],
    env_health: dict[str, Any],
    open_position_rows: list[dict[str, Any]],
    processed_signal_rows: list[dict[str, Any]],
    order_attempt_rows: list[dict[str, Any]],
    trade_history_rows: list[dict[str, Any]],
    signal_observation_rows: list[dict[str, Any]],
    exchange_position_rows: list[dict[str, Any]] | None = None,
    exchange_open_order_rows: list[dict[str, Any]] | None = None,
    funding_snapshot: Any | None = None,
    external_issue_rows: list[dict[str, Any]] | None = None,
    state_db_path: str | Path | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    soak_cfg = config.get("paper_soak", {})
    required_mode = str(soak_cfg.get("required_mode", "PAPER")).strip().upper()
    min_hours = _safe_float(soak_cfg.get("min_hours"), 24.0)
    min_order_attempts = _safe_int(soak_cfg.get("min_order_attempts"), 10)
    min_processed_signals = _safe_int(soak_cfg.get("min_processed_signals"), 10)
    min_signal_observations = _safe_int(soak_cfg.get("min_signal_observations"), 50)
    max_last_event_age_minutes = _safe_float(soak_cfg.get("max_last_event_age_minutes"), 60.0)
    max_event_gap_minutes = _safe_float(soak_cfg.get("max_event_gap_minutes"), 120.0)
    max_error_attempts = _safe_int(soak_cfg.get("max_error_attempts"), 0)
    max_unknown_attempts = _safe_int(soak_cfg.get("max_unknown_attempts"), 0)
    max_error_signals = _safe_int(soak_cfg.get("max_error_signals"), 0)
    require_live_readiness = _bool_or_default(soak_cfg.get("require_live_readiness"), True)
    require_isolated_db = _bool_or_default(soak_cfg.get("require_isolated_db"), True)
    state_db_path = state_db_path or config.get("state", {}).get("db_path")
    state_db_path_text = str(state_db_path or "")

    resolved_mode = resolve_execution_mode(config)
    soak_window = _build_soak_window(
        processed_signal_rows,
        order_attempt_rows,
        trade_history_rows,
        signal_observation_rows,
    )
    now = now.astimezone(timezone.utc) if now is not None else datetime.now(timezone.utc)
    ended_at = _parse_timestamp(soak_window.ended_at)
    last_event_age_minutes = None
    if ended_at is not None:
        last_event_age_minutes = max((now - ended_at).total_seconds() / 60.0, 0.0)

    attempt_status_counts: dict[str, int] = {}
    for row in order_attempt_rows:
        status = str(row.get("status") or "UNKNOWN")
        attempt_status_counts[status] = attempt_status_counts.get(status, 0) + 1

    signal_status_counts: dict[str, int] = {}
    for row in processed_signal_rows:
        status = str(row.get("status") or "UNKNOWN")
        signal_status_counts[status] = signal_status_counts.get(status, 0) + 1

    error_attempts = sum(
        count for status, count in attempt_status_counts.items() if status in ERROR_ATTEMPT_STATUSES
    )
    unknown_attempts = sum(
        count for status, count in attempt_status_counts.items() if status in UNKNOWN_ATTEMPT_STATUSES
    )
    processing_signals = signal_status_counts.get("PROCESSING", 0)
    error_signals = sum(
        count for status, count in signal_status_counts.items() if status in ERROR_SIGNAL_STATUSES
    )

    live_readiness = build_live_readiness_report(
        config=config,
        env_health=env_health,
        open_position_rows=open_position_rows,
        processed_signal_rows=processed_signal_rows,
        order_attempt_rows=order_attempt_rows,
        trade_history_rows=trade_history_rows,
        exchange_position_rows=exchange_position_rows,
        exchange_open_order_rows=exchange_open_order_rows,
        funding_snapshot=funding_snapshot,
        external_issue_rows=external_issue_rows,
        state_db_path=state_db_path,
    )

    blockers: list[str] = []
    warnings: list[str] = []

    if resolved_mode != required_mode:
        blockers.append(f"soak must run in {required_mode}; current mode is {resolved_mode}")

    if require_isolated_db:
        if not state_db_path_text:
            blockers.append("paper soak requires an explicit isolated state DB path")
        elif Path(state_db_path_text) == DEFAULT_DB_PATH or state_db_path_text.endswith(
            DEFAULT_DB_PATH.as_posix()
        ):
            blockers.append(
                "paper soak uses the shared default state DB; set POLY_EXECUTOR_DB_PATH "
                "or [state].db_path to an isolated paper DB"
            )

    if soak_window.hours < min_hours:
        blockers.append(f"soak window {soak_window.hours:.2f}h below required {min_hours:.2f}h")

    if last_event_age_minutes is None:
        blockers.append("soak has no timestamped events")
    elif last_event_age_minutes > max_last_event_age_minutes:
        blockers.append(
            f"last soak event age {last_event_age_minutes:.1f}m above allowed "
            f"{max_last_event_age_minutes:.1f}m"
        )

    if soak_window.event_count > 1 and soak_window.max_gap_minutes > max_event_gap_minutes:
        blockers.append(
            f"max soak event gap {soak_window.max_gap_minutes:.1f}m above allowed "
            f"{max_event_gap_minutes:.1f}m"
        )

    if len(order_attempt_rows) < min_order_attempts:
        blockers.append(
            f"order attempts {len(order_attempt_rows)} below required {min_order_attempts}"
        )

    if len(processed_signal_rows) < min_processed_signals:
        blockers.append(
            f"processed signals {len(processed_signal_rows)} below required {min_processed_signals}"
        )

    if len(signal_observation_rows) < min_signal_observations:
        blockers.append(
            f"signal observations {len(signal_observation_rows)} below required {min_signal_observations}"
        )

    if error_attempts > max_error_attempts:
        blockers.append(f"error order attempts {error_attempts} above allowed {max_error_attempts}")

    if unknown_attempts > max_unknown_attempts:
        blockers.append(f"unknown order attempts {unknown_attempts} above allowed {max_unknown_attempts}")

    if processing_signals > 0:
        blockers.append(f"processed_signals has {processing_signals} stuck PROCESSING rows")

    if error_signals > max_error_signals:
        blockers.append(f"execution error signals {error_signals} above allowed {max_error_signals}")

    if require_live_readiness and live_readiness["readiness_status"] != "GO":
        blockers.append("live readiness gate is not GO")

    if not order_attempt_rows:
        warnings.append("no order attempts in soak window")

    if not trade_history_rows:
        warnings.append("no trade history rows in soak window")

    status = "GO" if not blockers else "NO_GO"

    return {
        "cutover_status": status,
        "blockers": blockers,
        "warnings": warnings,
        "soak": {
            "required_mode": required_mode,
            "resolved_mode": resolved_mode,
            "state_db_path": state_db_path_text,
            "require_isolated_db": require_isolated_db,
            "min_hours": min_hours,
            "max_last_event_age_minutes": max_last_event_age_minutes,
            "max_event_gap_minutes": max_event_gap_minutes,
            "started_at": soak_window.started_at,
            "ended_at": soak_window.ended_at,
            "last_event_age_minutes": (
                round(last_event_age_minutes, 4)
                if last_event_age_minutes is not None
                else None
            ),
            "hours": soak_window.hours,
            "max_gap_minutes": soak_window.max_gap_minutes,
            "event_count": soak_window.event_count,
        },
        "counts": {
            "processed_signals": len(processed_signal_rows),
            "order_attempts": len(order_attempt_rows),
            "trade_history_rows": len(trade_history_rows),
            "signal_observations": len(signal_observation_rows),
            "error_attempts": error_attempts,
            "unknown_attempts": unknown_attempts,
            "processing_signals": processing_signals,
            "error_signals": error_signals,
            "attempt_status_counts": attempt_status_counts,
            "signal_status_counts": signal_status_counts,
        },
        "live_readiness": {
            "readiness_status": live_readiness["readiness_status"],
            "blockers": live_readiness["blockers"],
            "warnings": live_readiness["warnings"],
            "config_safety": live_readiness.get("config_safety", {}),
        },
    }
