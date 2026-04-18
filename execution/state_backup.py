from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import execution.state_store as state_store


@dataclass(frozen=True)
class BackupResult:
    created: bool
    reason: str
    source_path: str
    backup_path: str | None = None


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


def backup_state_db(
    *,
    config: dict[str, Any],
    label: str,
    db_path: Path | None = None,
) -> BackupResult:
    backup_cfg = config.get("state_backup", {})
    enabled = _bool_or_default(backup_cfg.get("enabled"), False)
    source = db_path or state_store.DB_PATH

    if not enabled:
        return BackupResult(False, "state backup disabled by config", source.as_posix())

    if not source.exists():
        return BackupResult(False, "state DB does not exist", source.as_posix())

    backup_dir = Path(str(backup_cfg.get("dir") or "data/backups"))
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_label = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in label)
    backup_path = backup_dir / f"{source.stem}_{safe_label}_{timestamp}{source.suffix}"
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, backup_path)

    return BackupResult(
        True,
        "ok",
        source.as_posix(),
        backup_path.as_posix(),
    )
