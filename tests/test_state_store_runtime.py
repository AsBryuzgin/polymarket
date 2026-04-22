from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import execution.state_store as state_store
from execution.state_store import DEFAULT_DB_PATH, resolve_state_db_path


class StateStoreRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self._original_db_path = state_store.DB_PATH

    def tearDown(self) -> None:
        state_store.DB_PATH = self._original_db_path

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

    def test_delete_leader_registry_row_removes_flat_leader(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_store.DB_PATH = Path(tmp) / "executor_state.db"
            state_store.init_db()
            state_store.upsert_leader_registry_row(
                wallet="wallet1",
                category="CRYPTO",
                user_name="Leader",
                leader_status="EXIT_ONLY",
                target_weight=0.0,
                target_budget_usd=0.0,
                grace_until=None,
                source_tag="test",
            )

            self.assertIsNotNone(state_store.get_leader_registry("wallet1"))

            state_store.delete_leader_registry_row("wallet1")

            self.assertIsNone(state_store.get_leader_registry("wallet1"))


if __name__ == "__main__":
    unittest.main()
