from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import execution.state_store as state_store
from execution.order_router import resolve_execution_mode


@dataclass(frozen=True)
class RuntimeGuardDecision:
    allowed: bool
    reason: str
    mode: str
    state_db_path: str
    details: dict[str, Any] = field(default_factory=dict)


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


def _is_default_state_db(path: Path) -> bool:
    path_text = path.as_posix()
    return path == state_store.DEFAULT_DB_PATH or path_text.endswith(
        state_store.DEFAULT_DB_PATH.as_posix()
    )


def evaluate_runtime_guard(
    *,
    config: dict[str, Any],
    state_db_path: Path | None = None,
) -> RuntimeGuardDecision:
    mode = resolve_execution_mode(config)
    path = state_db_path or state_store.DB_PATH
    guard_cfg = config.get("runtime_guard", {})
    require_paper_isolated = _bool_or_default(
        guard_cfg.get("require_isolated_db_for_paper"),
        True,
    )
    require_live_isolated = _bool_or_default(
        guard_cfg.get("require_isolated_db_for_live"),
        True,
    )

    details = {
        "require_isolated_db_for_paper": require_paper_isolated,
        "require_isolated_db_for_live": require_live_isolated,
        "default_state_db_path": state_store.DEFAULT_DB_PATH.as_posix(),
    }

    if mode == "PAPER" and require_paper_isolated and _is_default_state_db(path):
        return RuntimeGuardDecision(
            allowed=False,
            reason="PAPER requires an isolated state DB; current DB is the shared default",
            mode=mode,
            state_db_path=path.as_posix(),
            details=details,
        )

    if mode == "LIVE" and require_live_isolated and _is_default_state_db(path):
        return RuntimeGuardDecision(
            allowed=False,
            reason="LIVE requires an isolated state DB; current DB is the shared default",
            mode=mode,
            state_db_path=path.as_posix(),
            details=details,
        )

    return RuntimeGuardDecision(
        allowed=True,
        reason="ok",
        mode=mode,
        state_db_path=path.as_posix(),
        details=details,
    )
