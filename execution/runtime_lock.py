from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_LOCK_PATH = Path("data/runtime_trading.lock")


@dataclass(frozen=True)
class RuntimeLockState:
    locked: bool
    reason: str
    path: str
    payload: dict[str, Any] = field(default_factory=dict)


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


def runtime_lock_path(config: dict[str, Any]) -> Path:
    lock_cfg = config.get("runtime_lock", {})
    return Path(str(lock_cfg.get("path") or DEFAULT_LOCK_PATH))


def runtime_lock_enabled(config: dict[str, Any]) -> bool:
    lock_cfg = config.get("runtime_lock", {})
    return _bool_or_default(lock_cfg.get("enabled"), True)


def runtime_lock_activate_on_critical(config: dict[str, Any]) -> bool:
    lock_cfg = config.get("runtime_lock", {})
    return _bool_or_default(lock_cfg.get("activate_on_critical_alerts"), True)


def read_runtime_lock(config: dict[str, Any]) -> RuntimeLockState:
    path = runtime_lock_path(config)
    if not runtime_lock_enabled(config):
        return RuntimeLockState(False, "runtime lock disabled by config", path.as_posix())

    if not path.exists():
        return RuntimeLockState(False, "no runtime lock", path.as_posix())

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        payload = {}

    reason = str(payload.get("reason") or "runtime trading lock is active")
    return RuntimeLockState(True, reason, path.as_posix(), payload)


def activate_runtime_lock(
    config: dict[str, Any],
    *,
    reason: str,
    source: str,
    alerts: list[dict[str, Any]] | None = None,
) -> RuntimeLockState:
    path = runtime_lock_path(config)
    payload = {
        "locked": True,
        "reason": reason,
        "source": source,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "alerts": alerts or [],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return RuntimeLockState(True, reason, path.as_posix(), payload)


def clear_runtime_lock(config: dict[str, Any]) -> RuntimeLockState:
    path = runtime_lock_path(config)
    if path.exists():
        path.unlink()
    return RuntimeLockState(False, "runtime lock cleared", path.as_posix())
