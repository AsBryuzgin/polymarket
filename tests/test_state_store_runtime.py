from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from execution.state_store import DEFAULT_DB_PATH, resolve_state_db_path


class StateStoreRuntimeTests(unittest.TestCase):
    def test_env_db_path_overrides_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "executor.toml"
            config_path.write_text(
                "[state]\ndb_path = 'data/from_config.db'\n",
                encoding="utf-8",
            )

            db_path = resolve_state_db_path(
                config_path=config_path,
                env={"POLY_EXECUTOR_DB_PATH": "data/from_env.db"},
            )

        self.assertEqual(db_path, Path("data/from_env.db"))

    def test_config_db_path_is_used_when_env_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "executor.toml"
            config_path.write_text(
                "[state]\ndb_path = 'data/from_config.db'\n",
                encoding="utf-8",
            )

            db_path = resolve_state_db_path(config_path=config_path, env={})

        self.assertEqual(db_path, Path("data/from_config.db"))

    def test_missing_config_uses_default_db_path(self) -> None:
        db_path = resolve_state_db_path(
            config_path="/tmp/definitely-missing-executor-config.toml",
            env={},
        )

        self.assertEqual(db_path, DEFAULT_DB_PATH)


if __name__ == "__main__":
    unittest.main()
