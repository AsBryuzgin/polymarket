from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import execution.state_store as state_store
from execution.manual_unwind import (
    build_unwind_preview,
    execute_manual_unwind,
    format_unwind_result,
    list_unwind_targets,
)


class ManualUnwindTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self._original_db_path = state_store.DB_PATH
        state_store.DB_PATH = Path(self.tmpdir.name) / "executor_state.db"
        state_store.init_db()

    def tearDown(self) -> None:
        state_store.DB_PATH = self._original_db_path
        self.tmpdir.cleanup()

    def _config(self, *, preview: bool = True) -> dict:
        return {
            "global": {
                "preview_mode": preview,
                "simulation": False,
                "execution_mode": "PREVIEW" if preview else "PAPER",
            },
            "runtime_guard": {
                "require_isolated_db_for_paper": True,
                "require_isolated_db_for_live": True,
            },
            "state_backup": {"enabled": False},
            "alert_delivery": {"enabled": False, "notify_trades": True},
        }

    def test_targets_group_open_positions_by_leader(self) -> None:
        state_store.upsert_leader_registry_row(
            wallet="wallet1",
            category="SPORTS",
            user_name="Leader One",
            leader_status="ACTIVE",
            target_weight=1.0,
            target_budget_usd=10.0,
            grace_until=None,
            source_tag="test",
        )
        state_store.upsert_buy_position("wallet1", "tokenA", 2.0, 0.25, "buy1")
        state_store.upsert_buy_position("wallet1", "tokenB", 3.0, 0.50, "buy2")

        targets = list_unwind_targets()
        preview = build_unwind_preview("wallet1")

        self.assertEqual(len(targets), 1)
        self.assertEqual(targets[0]["user_name"], "Leader One")
        self.assertEqual(targets[0]["positions"], 2)
        self.assertEqual(targets[0]["position_usd"], 5.0)
        self.assertEqual(preview["positions"], 2)
        self.assertEqual(preview["position_usd"], 5.0)

    def test_manual_unwind_sells_share_quantity_and_closes_cost_basis(self) -> None:
        calls = []
        notifications = []
        state_store.upsert_leader_registry_row(
            wallet="wallet1",
            category="SPORTS",
            user_name="Leader One",
            leader_status="ACTIVE",
            target_weight=1.0,
            target_budget_usd=10.0,
            grace_until=None,
            source_tag="test",
        )
        state_store.upsert_buy_position("wallet1", "tokenA", 2.0, 0.25, "buy1")

        def snapshot_loader(token_id: str, side: str) -> dict:
            self.assertEqual(token_id, "tokenA")
            self.assertEqual(side, "SELL")
            return {
                "midpoint": 0.51,
                "price_quote": 0.50,
                "best_bid": 0.50,
                "best_ask": 0.52,
                "spread": 0.02,
            }

        def preview_share_fn(*, token_id: str, share_amount: float, side: str) -> dict:
            calls.append({"token_id": token_id, "share_amount": share_amount, "side": side})
            return {"ok": True, "share_amount": share_amount, "side": side}

        summary = execute_manual_unwind(
            target_wallet="wallet1",
            config=self._config(preview=True),
            snapshot_loader=snapshot_loader,
            preview_share_fn=preview_share_fn,
            live_share_fn=lambda **kwargs: {"unused": True},
            notification_fn=lambda **kwargs: notifications.append(kwargs),
        )

        self.assertEqual(summary["status"], "OK")
        self.assertEqual(summary["success"], 1)
        self.assertEqual(calls[0]["share_amount"], 8.0)
        self.assertIsNone(state_store.get_open_position("wallet1", "tokenA"))
        self.assertEqual(summary["results"][0]["proceeds_usd"], 4.0)
        self.assertEqual(summary["results"][0]["realized_pnl_usd"], 2.0)
        self.assertEqual(notifications[0]["amount_usd"], 2.0)
        self.assertEqual(notifications[0]["price"], 0.5)

        history = state_store.list_trade_history(limit=10)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["event_type"], "EXIT")
        self.assertEqual(float(history[0]["amount_usd"]), 2.0)
        self.assertEqual(float(history[0]["gross_value_usd"]), 4.0)
        self.assertEqual(float(history[0]["realized_pnl_usd"]), 2.0)

        message = format_unwind_result(summary)
        self.assertIn("filled: 1", message)
        self.assertIn("PnL +$2.00", message)

    def test_manual_unwind_skips_settled_position_for_settlement_workflow(self) -> None:
        state_store.upsert_buy_position("wallet1", "tokenA", 2.0, 0.25, "buy1")

        from unittest.mock import patch

        with patch(
            "execution.manual_unwind.mark_position",
            return_value={
                "snapshot_status": "SETTLED",
                "snapshot_reason": "settlement fallback from resolved market",
            },
        ):
            summary = execute_manual_unwind(
                target_wallet="wallet1",
                config=self._config(preview=True),
                snapshot_loader=lambda _token_id, _side: {},
                preview_share_fn=lambda **kwargs: {"unused": True},
                live_share_fn=lambda **kwargs: {"unused": True},
                notification_fn=lambda **kwargs: None,
            )

        self.assertEqual(summary["status"], "PARTIAL")
        self.assertEqual(summary["success"], 0)
        self.assertEqual(summary["results"][0]["status"], "SKIPPED_SETTLEMENT_REQUIRED")
        self.assertIsNotNone(state_store.get_open_position("wallet1", "tokenA"))


if __name__ == "__main__":
    unittest.main()
