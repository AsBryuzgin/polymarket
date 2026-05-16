from __future__ import annotations

import os
import sys
from pathlib import Path
from pprint import pprint

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.builder_auth import EXECUTOR_CONFIG_ENV_VAR, load_executor_config
from execution.order_router import resolve_execution_mode
import execution.state_store as state_store
from execution.state_store import (
    init_db,
    list_open_positions,
    list_order_attempts,
    list_leader_registry,
    list_processed_signals,
    list_trade_history,
)


def main() -> None:
    config = load_executor_config()
    mode = resolve_execution_mode(config)
    configured_path = config.get("state", {}).get("db_path")
    env_path = os.getenv(state_store.DB_PATH_ENV_VAR)
    warnings: list[str] = []

    if mode in {"PAPER", "LIVE"} and state_store.DB_PATH == state_store.DEFAULT_DB_PATH:
        warnings.append(
            f"{mode} is using the shared default state DB; set {state_store.DB_PATH_ENV_VAR} "
            "or [state].db_path before soak/live"
        )

    init_db()

    report = {
        "mode": mode,
        "executor_config_path": os.getenv(EXECUTOR_CONFIG_ENV_VAR, "config/executor.toml"),
        "state_db_path": str(state_store.DB_PATH),
        "state_db_exists": state_store.DB_PATH.exists(),
        "state_db_size_bytes": (
            state_store.DB_PATH.stat().st_size if state_store.DB_PATH.exists() else 0
        ),
        "configured_state_db_path": configured_path or "",
        "env_state_db_path": env_path or "",
        "counts": {
            "leader_registry": len(list_leader_registry(limit=100000)),
            "open_positions": len(list_open_positions(limit=100000)),
            "processed_signals": len(list_processed_signals(limit=100000)),
            "order_attempts": len(list_order_attempts(limit=100000)),
            "trade_history_rows": len(list_trade_history(limit=100000)),
        },
        "warnings": warnings,
    }

    print("=== RUNTIME STATE CHECK ===")
    pprint(report)


if __name__ == "__main__":
    main()
