from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from execution.cutover_readiness import (
    ERROR_ATTEMPT_STATUSES,
    ERROR_SIGNAL_STATUSES,
    UNKNOWN_ATTEMPT_STATUSES,
    _build_soak_window,
    _parse_timestamp,
    _safe_float,
    _safe_int,
)
from execution.order_router import resolve_execution_mode


def _status_counts(rows: list[dict[str, Any]], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        status = str(row.get(field) or "UNKNOWN")
        counts[status] = counts.get(status, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: item[1], reverse=True))


def _progress(actual: float, required: float) -> dict[str, Any]:
    if required <= 0:
        ratio = 1.0
    else:
        ratio = min(actual / required, 1.0)
    return {
        "actual": round(actual, 6),
        "required": required,
        "ok": actual >= required,
        "progress": round(ratio, 6),
    }


def flatten_paper_soak_status_report(report: dict[str, Any]) -> dict[str, Any]:
    soak_window = report.get("soak_window", {})
    counts = report.get("counts", {})
    registry = report.get("registry", {})
    mode = report.get("mode", {})
    progress = report.get("progress", {})
    status_counts = report.get("status_counts", {})

    flattened = {
        "soak_status": report.get("soak_status"),
        "blockers": "; ".join(report.get("blockers") or []),
        "resolved_mode": mode.get("resolved_mode"),
        "state_db_path": mode.get("state_db_path"),
        "leaders": registry.get("leaders", 0),
        "active_leaders": registry.get("active_leaders", 0),
        "exit_only_leaders": registry.get("exit_only_leaders", 0),
        "started_at": soak_window.get("started_at"),
        "ended_at": soak_window.get("ended_at"),
        "hours": soak_window.get("hours", 0.0),
        "event_count": soak_window.get("event_count", 0),
        "last_event_age_minutes": soak_window.get("last_event_age_minutes"),
        "last_event_fresh": soak_window.get("last_event_fresh"),
        "max_gap_minutes": soak_window.get("max_gap_minutes", 0.0),
        "event_gap_ok": soak_window.get("event_gap_ok"),
        "open_positions": counts.get("open_positions", 0),
        "processed_signals": counts.get("processed_signals", 0),
        "order_attempts": counts.get("order_attempts", 0),
        "trade_history_rows": counts.get("trade_history_rows", 0),
        "signal_observations": counts.get("signal_observations", 0),
        "selected_observations": counts.get("selected_observations", 0),
        "error_attempts": counts.get("error_attempts", 0),
        "unknown_attempts": counts.get("unknown_attempts", 0),
        "error_signals": counts.get("error_signals", 0),
        "processing_signals": counts.get("processing_signals", 0),
        "hours_progress": progress.get("hours", {}).get("progress", 0.0),
        "order_attempts_progress": progress.get("order_attempts", {}).get("progress", 0.0),
        "processed_signals_progress": progress.get("processed_signals", {}).get("progress", 0.0),
        "signal_observations_progress": progress.get("signal_observations", {}).get("progress", 0.0),
    }

    for prefix, counts_map in (
        ("observation_status", status_counts.get("observations", {})),
        ("processed_signal_status", status_counts.get("processed_signals", {})),
        ("order_attempt_status", status_counts.get("order_attempts", {})),
    ):
        for status, count in counts_map.items():
            flattened[f"{prefix}:{status}"] = count

    return flattened


def build_paper_soak_status_report(
    *,
    config: dict[str, Any],
    leader_registry_rows: list[dict[str, Any]],
    open_position_rows: list[dict[str, Any]],
    processed_signal_rows: list[dict[str, Any]],
    order_attempt_rows: list[dict[str, Any]],
    trade_history_rows: list[dict[str, Any]],
    signal_observation_rows: list[dict[str, Any]],
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

    mode = resolve_execution_mode(config)
    now = now.astimezone(timezone.utc) if now is not None else datetime.now(timezone.utc)
    soak_window = _build_soak_window(
        processed_signal_rows,
        order_attempt_rows,
        trade_history_rows,
        signal_observation_rows,
    )
    ended_at = _parse_timestamp(soak_window.ended_at)
    last_event_age_minutes = None
    if ended_at is not None:
        last_event_age_minutes = max((now - ended_at).total_seconds() / 60.0, 0.0)

    attempt_status_counts = _status_counts(order_attempt_rows, "status")
    signal_status_counts = _status_counts(processed_signal_rows, "status")
    observation_status_counts = _status_counts(signal_observation_rows, "latest_status")

    error_attempts = sum(
        count for status, count in attempt_status_counts.items() if status in ERROR_ATTEMPT_STATUSES
    )
    unknown_attempts = sum(
        count for status, count in attempt_status_counts.items() if status in UNKNOWN_ATTEMPT_STATUSES
    )
    error_signals = sum(
        count for status, count in signal_status_counts.items() if status in ERROR_SIGNAL_STATUSES
    )
    processing_signals = signal_status_counts.get("PROCESSING", 0)

    last_event_fresh = (
        last_event_age_minutes is not None
        and last_event_age_minutes <= max_last_event_age_minutes
    )
    event_gap_ok = (
        soak_window.event_count <= 1
        or soak_window.max_gap_minutes <= max_event_gap_minutes
    )

    progress = {
        "hours": _progress(soak_window.hours, min_hours),
        "order_attempts": _progress(float(len(order_attempt_rows)), float(min_order_attempts)),
        "processed_signals": _progress(float(len(processed_signal_rows)), float(min_processed_signals)),
        "signal_observations": _progress(float(len(signal_observation_rows)), float(min_signal_observations)),
    }

    blockers: list[str] = []
    if mode != required_mode:
        blockers.append(f"mode is {mode}, expected {required_mode}")
    if not leader_registry_rows:
        blockers.append("leader registry is empty")
    if error_attempts > max_error_attempts:
        blockers.append(f"error attempts {error_attempts} above allowed {max_error_attempts}")
    if unknown_attempts > max_unknown_attempts:
        blockers.append(f"unknown attempts {unknown_attempts} above allowed {max_unknown_attempts}")
    if error_signals > max_error_signals:
        blockers.append(f"error signals {error_signals} above allowed {max_error_signals}")
    if processing_signals:
        blockers.append(f"processing signals {processing_signals} above allowed 0")

    all_minimums_met = all(item["ok"] for item in progress.values())
    if not signal_observation_rows and not order_attempt_rows and not processed_signal_rows:
        status = "EMPTY"
    elif blockers:
        status = "BLOCKED"
    elif all_minimums_met and last_event_fresh and event_gap_ok:
        status = "READY_FOR_CUTOVER_CHECK"
    else:
        status = "RUNNING"

    return {
        "soak_status": status,
        "blockers": blockers,
        "mode": {
            "required_mode": required_mode,
            "resolved_mode": mode,
            "state_db_path": str(state_db_path or config.get("state", {}).get("db_path") or ""),
        },
        "registry": {
            "leaders": len(leader_registry_rows),
            "active_leaders": sum(
                1 for row in leader_registry_rows if row.get("leader_status") == "ACTIVE"
            ),
            "exit_only_leaders": sum(
                1 for row in leader_registry_rows if row.get("leader_status") == "EXIT_ONLY"
            ),
        },
        "soak_window": {
            "started_at": soak_window.started_at,
            "ended_at": soak_window.ended_at,
            "hours": soak_window.hours,
            "event_count": soak_window.event_count,
            "last_event_age_minutes": (
                round(last_event_age_minutes, 6) if last_event_age_minutes is not None else None
            ),
            "last_event_fresh": last_event_fresh,
            "max_gap_minutes": soak_window.max_gap_minutes,
            "max_event_gap_minutes": max_event_gap_minutes,
            "event_gap_ok": event_gap_ok,
        },
        "progress": progress,
        "counts": {
            "open_positions": len(open_position_rows),
            "processed_signals": len(processed_signal_rows),
            "order_attempts": len(order_attempt_rows),
            "trade_history_rows": len(trade_history_rows),
            "signal_observations": len(signal_observation_rows),
            "selected_observations": sum(
                1 for row in signal_observation_rows if row.get("selected_signal_id")
            ),
            "error_attempts": error_attempts,
            "unknown_attempts": unknown_attempts,
            "error_signals": error_signals,
            "processing_signals": processing_signals,
        },
        "status_counts": {
            "observations": observation_status_counts,
            "processed_signals": signal_status_counts,
            "order_attempts": attempt_status_counts,
        },
    }
