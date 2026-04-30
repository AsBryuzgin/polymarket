from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import execution.state_store as state_store
from execution.copy_worker import LeaderSignal, process_signal
from execution.runtime_guard import evaluate_runtime_guard


class RuntimeGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.original_db_path = state_store.DB_PATH
        self.db_path = Path(self.tmpdir.name) / "test_executor_state.db"
        state_store.DB_PATH = self.db_path
        state_store.init_db()

    def tearDown(self) -> None:
        state_store.DB_PATH = self.original_db_path
        self.tmpdir.cleanup()

    def test_preview_mode_allows_default_state_db(self) -> None:
        decision = evaluate_runtime_guard(
            config={"global": {"simulation": True, "preview_mode": True}},
            state_db_path=state_store.DEFAULT_DB_PATH,
        )

        self.assertTrue(decision.allowed)

    def test_paper_mode_blocks_default_state_db(self) -> None:
        decision = evaluate_runtime_guard(
            config={
                "global": {
                    "simulation": True,
                    "preview_mode": False,
                    "execution_mode": "paper",
                }
            },
            state_db_path=state_store.DEFAULT_DB_PATH,
        )

        self.assertFalse(decision.allowed)
        self.assertIn("PAPER requires an isolated state DB", decision.reason)

    def test_live_mode_blocks_default_state_db(self) -> None:
        decision = evaluate_runtime_guard(
            config={
                "global": {
                    "simulation": False,
                    "preview_mode": False,
                    "execution_mode": "live",
                }
            },
            state_db_path=state_store.DEFAULT_DB_PATH,
        )

        self.assertFalse(decision.allowed)
        self.assertIn("LIVE requires an isolated state DB", decision.reason)

    def test_copy_worker_runtime_guard_blocks_before_market_fetch(self) -> None:
        signal = LeaderSignal(
            signal_id="sig-runtime-block",
            leader_wallet="wallet-runtime",
            token_id="token-runtime",
            side="BUY",
            leader_budget_usd=50.0,
            leader_trade_notional_usd=10.0,
        )
        config = {
            "global": {
                "simulation": True,
                "preview_mode": False,
                "execution_mode": "paper",
            }
        }

        with patch("execution.copy_worker.load_executor_config", return_value=config), \
             patch(
                 "execution.copy_worker.evaluate_runtime_guard",
                 return_value=evaluate_runtime_guard(
                     config=config,
                     state_db_path=state_store.DEFAULT_DB_PATH,
                 ),
             ), \
             patch("execution.copy_worker.fetch_market_snapshot") as fetch_snapshot:
            result = process_signal(signal)

        fetch_snapshot.assert_not_called()
        self.assertEqual(result["status"], "SKIPPED_RUNTIME")
        self.assertIn("PAPER requires an isolated state DB", result["reason"])
        self.assertTrue(state_store.has_signal("sig-runtime-block"))
        self.assertEqual(state_store.list_order_attempts("sig-runtime-block"), [])


if __name__ == "__main__":
    unittest.main()
