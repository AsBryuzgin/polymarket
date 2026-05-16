from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import execution.state_store as state_store
from execution.order_recovery import (
    RECOVERY_APPLY_ACK,
    apply_unverified_order_recovery,
    build_unverified_order_recovery_report,
)


class OrderRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.original_db_path = state_store.DB_PATH
        state_store.DB_PATH = Path(self.tmpdir.name) / "executor_state.db"
        state_store.init_db()

    def tearDown(self) -> None:
        state_store.DB_PATH = self.original_db_path
        self.tmpdir.cleanup()

    def test_recovery_report_verifies_fill_from_order_status(self) -> None:
        rows = build_unverified_order_recovery_report(
            order_attempt_rows=[
                {
                    "attempt_id": 1,
                    "signal_id": "sig1",
                    "leader_wallet": "wallet1",
                    "token_id": "tokenA",
                    "side": "BUY",
                    "amount_usd": 2.0,
                    "status": "LIVE_SUBMITTED_UNVERIFIED",
                    "order_id": "order1",
                }
            ],
            order_status_fetcher=lambda _order_id: {
                "success": True,
                "orderID": "order1",
                "filled_amount_usd": "1.5",
                "status": "FILLED",
            },
        )

        self.assertEqual(rows[0]["recovery_status"], "FILL_VERIFIED")
        self.assertEqual(rows[0]["verified_fill_amount_usd"], 1.5)

    def test_recovery_report_computes_fill_from_size_matched_and_price(self) -> None:
        rows = build_unverified_order_recovery_report(
            order_attempt_rows=[
                {
                    "attempt_id": 1,
                    "signal_id": "sig1",
                    "leader_wallet": "wallet1",
                    "token_id": "tokenA",
                    "side": "BUY",
                    "amount_usd": 2.0,
                    "status": "LIVE_SUBMITTED_UNVERIFIED",
                    "order_id": "order1",
                }
            ],
            order_status_fetcher=lambda _order_id: {
                "success": True,
                "orderID": "order1",
                "status": "FILLED",
                "price": "0.42",
                "size_matched": "5",
            },
        )

        self.assertEqual(rows[0]["recovery_status"], "FILL_VERIFIED")
        self.assertEqual(rows[0]["verified_fill_amount_usd"], 2.1)
        self.assertEqual(rows[0]["verified_fill_price"], 0.42)

    def test_recovery_report_handles_missing_order_id(self) -> None:
        rows = build_unverified_order_recovery_report(
            order_attempt_rows=[
                {
                    "attempt_id": 1,
                    "signal_id": "sig1",
                    "leader_wallet": "wallet1",
                    "token_id": "tokenA",
                    "side": "BUY",
                    "amount_usd": 2.0,
                    "status": "LIVE_SUBMITTED_UNVERIFIED",
                }
            ],
        )

        self.assertEqual(rows[0]["recovery_status"], "ORDER_ID_MISSING")

    def test_apply_recovery_requires_ack(self) -> None:
        with self.assertRaises(ValueError):
            apply_unverified_order_recovery(
                order_attempt_rows=[
                    {
                        "attempt_id": 1,
                        "signal_id": "sig1",
                        "leader_wallet": "wallet1",
                        "token_id": "tokenA",
                        "side": "BUY",
                        "amount_usd": 2.0,
                        "status": "LIVE_SUBMITTED_UNVERIFIED",
                        "order_id": "order1",
                    }
                ],
                apply=True,
                ack="",
            )

    def test_apply_recovery_updates_state_only_for_verified_fill_with_price(self) -> None:
        attempt_id = state_store.create_order_attempt(
            signal_id="sig-buy-live",
            leader_wallet="wallet1",
            token_id="tokenA",
            side="BUY",
            amount_usd=2.0,
            mode="LIVE",
            status="LIVE_SUBMITTED_UNVERIFIED",
            reason="submitted but not verified",
        )
        state_store.update_order_attempt(
            attempt_id=attempt_id,
            status="LIVE_SUBMITTED_UNVERIFIED",
            reason="submitted but not verified",
            raw_response={"orderID": "order1"},
            order_id="order1",
        )

        rows = apply_unverified_order_recovery(
            order_attempt_rows=state_store.list_order_attempts(limit=100),
            apply=True,
            ack=RECOVERY_APPLY_ACK,
            order_status_fetcher=lambda _order_id: {
                "success": True,
                "orderID": "order1",
                "status": "FILLED",
                "price": "0.50",
                "size_matched": "4",
            },
        )

        self.assertEqual(rows[0]["recovery_status"], "FILL_VERIFIED")
        self.assertTrue(rows[0]["applied"])
        self.assertEqual(rows[0]["signal_status"], "LIVE_FILLED_ENTRY")

        pos = state_store.get_open_position("wallet1", "tokenA")
        self.assertIsNotNone(pos)
        self.assertEqual(float(pos["position_usd"]), 2.0)
        self.assertEqual(float(pos["avg_entry_price"]), 0.50)

        attempts = state_store.list_order_attempts("sig-buy-live")
        self.assertEqual(attempts[0]["status"], "LIVE_FILLED_RECOVERED")
        self.assertEqual(float(attempts[0]["fill_amount_usd"]), 2.0)

        signals = state_store.list_processed_signals(limit=100)
        self.assertEqual(signals[0]["status"], "LIVE_FILLED_ENTRY")
        self.assertEqual(float(signals[0]["suggested_amount_usd"]), 2.0)

        history = state_store.list_trade_history(limit=10)
        self.assertEqual(history[0]["event_type"], "ENTRY")
        self.assertEqual(float(history[0]["amount_usd"]), 2.0)

    def test_apply_recovery_does_not_apply_without_fill_price(self) -> None:
        attempt_id = state_store.create_order_attempt(
            signal_id="sig-buy-no-price",
            leader_wallet="wallet1",
            token_id="tokenA",
            side="BUY",
            amount_usd=2.0,
            mode="LIVE",
            status="LIVE_SUBMITTED_UNVERIFIED",
            reason="submitted but not verified",
        )
        state_store.update_order_attempt(
            attempt_id=attempt_id,
            status="LIVE_SUBMITTED_UNVERIFIED",
            reason="submitted but not verified",
            raw_response={"orderID": "order1"},
            order_id="order1",
        )

        rows = apply_unverified_order_recovery(
            order_attempt_rows=state_store.list_order_attempts(limit=100),
            apply=True,
            ack=RECOVERY_APPLY_ACK,
            order_status_fetcher=lambda _order_id: {
                "success": True,
                "orderID": "order1",
                "status": "FILLED",
                "filled_amount_usd": "2.0",
            },
        )

        self.assertEqual(rows[0]["recovery_status"], "FILL_VERIFIED_PRICE_MISSING")
        self.assertFalse(rows[0]["applied"])
        self.assertIsNone(state_store.get_open_position("wallet1", "tokenA"))


if __name__ == "__main__":
    unittest.main()
