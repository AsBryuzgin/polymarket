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

    def test_processed_settlement_is_upserted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_store.DB_PATH = Path(tmp) / "executor_state.db"
            state_store.init_db()

            state_store.record_processed_settlement(
                "condition-1",
                market_slug="slug-a",
                question="Question A",
                token_ids=["tokenA"],
                mode="PAPER",
                status="PAPER_SETTLED",
                reason="ok",
                expected_payout_usd=10.0,
                position_count=1,
                raw_response={"ok": True},
            )
            row = state_store.get_processed_settlement("condition-1")
            self.assertIsNotNone(row)
            self.assertEqual(row["status"], "PAPER_SETTLED")

            state_store.record_processed_settlement(
                "condition-1",
                market_slug="slug-b",
                question="Question B",
                token_ids=["tokenA", "tokenB"],
                mode="LIVE",
                status="LIVE_SUBMITTED",
                reason="pending",
                transaction_id="tx-1",
            )
            row = state_store.get_processed_settlement("condition-1")
            self.assertIsNotNone(row)
            self.assertEqual(row["mode"], "LIVE")
            self.assertEqual(row["status"], "LIVE_SUBMITTED")
            self.assertEqual(row["transaction_id"], "tx-1")

    def test_close_position_and_log_trade_is_consistent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_store.DB_PATH = Path(tmp) / "executor_state.db"
            state_store.init_db()
            state_store.upsert_buy_position(
                leader_wallet="wallet1",
                token_id="tokenA",
                amount_usd=5.0,
                entry_price=0.5,
                signal_id="sig-entry",
            )

            result = state_store.close_position_and_log_trade(
                leader_wallet="wallet1",
                leader_user_name="Leader",
                category="SPORTS",
                leader_status="ACTIVE",
                token_id="tokenA",
                signal_id="settlement:condition-1",
                side="SELL",
                event_type="EXIT",
                price=1.0,
                gross_value_usd=10.0,
                exit_price=1.0,
                holding_minutes=12.0,
                notes="paper settlement redeem | condition_id=condition-1",
            )

            self.assertIsNotNone(result)
            self.assertEqual(state_store.list_open_positions(limit=10), [])
            history = state_store.list_trade_history(limit=10)
            self.assertEqual(len(history), 1)
            self.assertEqual(history[0]["signal_id"], "settlement:condition-1")
            self.assertEqual(history[0]["event_type"], "EXIT")
            self.assertAlmostEqual(float(history[0]["amount_usd"]), 5.0)
            self.assertAlmostEqual(float(history[0]["gross_value_usd"]), 10.0)
            self.assertAlmostEqual(float(history[0]["realized_pnl_usd"]), 5.0)


if __name__ == "__main__":
    unittest.main()
