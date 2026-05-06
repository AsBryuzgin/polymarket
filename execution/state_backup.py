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
    pruned_count: int = 0
    pruned_bytes: int = 0


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


def _int_or_default(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default


def _float_or_default(value: Any, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default


def _unique_backup_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    for idx in range(1, 1000):
        candidate = parent / f"{stem}_{idx}{suffix}"
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"could not find unique backup path for {path}")


def _backup_files_for_source(*, backup_dir: Path, source: Path) -> list[Path]:
    if not backup_dir.exists():
        return []
    pattern = f"{source.stem}_*{source.suffix}"
    return [path for path in backup_dir.glob(pattern) if path.is_file()]


def _file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _prune_backup_files(
    *,
    files: list[Path],
    keep_last: int,
    max_total_bytes: int,
    preserve: set[Path] | None = None,
) -> tuple[int, int]:
    preserve = {path.resolve() for path in (preserve or set())}
    existing = [path for path in files if path.exists()]
    existing.sort(key=lambda path: path.stat().st_ctime_ns, reverse=True)

    to_delete: list[Path] = []
    for path in existing:
        if _file_size(path) == 0 and path.resolve() not in preserve:
            to_delete.append(path)

    existing = [path for path in existing if path not in to_delete]
    if keep_last > 0:
        for path in existing[keep_last:]:
            if path.resolve() not in preserve:
                to_delete.append(path)

    remaining = [path for path in existing if path not in to_delete]
    if max_total_bytes > 0:
        total_bytes = sum(_file_size(path) for path in remaining)
        for path in sorted(remaining, key=lambda item: item.stat().st_ctime_ns):
            if total_bytes <= max_total_bytes:
                break
            if len(remaining) <= 1:
                break
            if path.resolve() in preserve:
                continue
            to_delete.append(path)
            remaining.remove(path)
            total_bytes -= _file_size(path)

    pruned_count = 0
    pruned_bytes = 0
    seen: set[Path] = set()
    for path in to_delete:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        size = _file_size(path)
        try:
            path.unlink()
        except FileNotFoundError:
            continue
        pruned_count += 1
        pruned_bytes += size

    return pruned_count, pruned_bytes


def prune_state_backups(
    *,
    config: dict[str, Any],
    db_path: Path | None = None,
    preserve: set[Path] | None = None,
) -> tuple[int, int]:
    backup_cfg = config.get("state_backup", {})
    source = db_path or state_store.DB_PATH
    backup_dir = Path(str(backup_cfg.get("dir") or "data/backups"))
    keep_last = _int_or_default(backup_cfg.get("retention_keep_last"), 6)
    max_total_mb = _float_or_default(backup_cfg.get("retention_max_total_mb"), 2048.0)
    max_total_bytes = int(max_total_mb * 1024 * 1024)
    return _prune_backup_files(
        files=_backup_files_for_source(backup_dir=backup_dir, source=source),
        keep_last=keep_last,
        max_total_bytes=max_total_bytes,
        preserve=preserve,
    )


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
    backup_dir.mkdir(parents=True, exist_ok=True)
    pruned_count, pruned_bytes = prune_state_backups(
        config=config,
        db_path=source,
    )

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_label = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in label)
    backup_path = _unique_backup_path(
        backup_dir / f"{source.stem}_{safe_label}_{timestamp}{source.suffix}"
    )
    try:
        shutil.copy2(source, backup_path)
    except OSError as exc:
        try:
            backup_path.unlink()
        except FileNotFoundError:
            pass
        return BackupResult(
            False,
            f"state backup failed: {exc}",
            source.as_posix(),
            backup_path.as_posix(),
            pruned_count=pruned_count,
            pruned_bytes=pruned_bytes,
        )

    after_count, after_bytes = prune_state_backups(
        config=config,
        db_path=source,
        preserve={backup_path},
    )

    return BackupResult(
        True,
        "ok",
        source.as_posix(),
        backup_path.as_posix(),
        pruned_count=pruned_count + after_count,
        pruned_bytes=pruned_bytes + after_bytes,
    )
