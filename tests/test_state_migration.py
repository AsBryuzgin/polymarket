from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import execution.state_store as state_store
from execution.state_migration import (
    apply_legacy_order_attempt_backfill,
    plan_legacy_order_attempt_backfill,
)


class StateMigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "test_executor_state.db"
        state_store.DB_PATH = self.db_path
        state_store.init_db()

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_plan_legacy_order_attempt_backfill_for_preview_signal(self) -> None:
        plan = plan_legacy_order_attempt_backfill(
            processed_signal_rows=[
                {
                    "signal_id": "sig1",
                    "leader_wallet": "wallet1",
                    "token_id": "tokenA",
                    "side": "BUY",
                    "status": "PREVIEW_READY_ENTRY",
                    "suggested_amount_usd": 2.0,
                }
            ],
            order_attempt_rows=[],
        )

        self.assertEqual(len(plan), 1)
        self.assertEqual(plan[0].mode, "PREVIEW")
        self.assertEqual(plan[0].attempt_status, "PREVIEW_READY")

    def test_plan_skips_live_filled_without_attempt(self) -> None:
        plan = plan_legacy_order_attempt_backfill(
            processed_signal_rows=[
                {
                    "signal_id": "sig1",
                    "leader_wallet": "wallet1",
                    "token_id": "tokenA",
                    "side": "BUY",
                    "status": "LIVE_FILLED_ENTRY",
                    "suggested_amount_usd": 2.0,
                }
            ],
            order_attempt_rows=[],
        )

        self.assertEqual(plan, [])

    def test_apply_legacy_order_attempt_backfill(self) -> None:
        state_store.record_signal(
            signal_id="sig1",
            leader_wallet="wallet1",
            token_id="tokenA",
            side="BUY",
            leader_budget_usd=10.0,
            suggested_amount_usd=2.0,
            status="PREVIEW_READY_ENTRY",
            reason="ok",
        )

        applied = apply_legacy_order_attempt_backfill()
        attempts = state_store.list_order_attempts(signal_id="sig1")

        self.assertEqual(len(applied), 1)
        self.assertEqual(len(attempts), 1)
        self.assertEqual(attempts[0]["mode"], "PREVIEW")
        self.assertEqual(attempts[0]["status"], "PREVIEW_READY")
        self.assertEqual(float(attempts[0]["fill_amount_usd"]), 2.0)


if __name__ == "__main__":
    unittest.main()
