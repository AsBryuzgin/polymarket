from __future__ import annotations

import unittest

from execution.reconciliation import reconcile_executor_state


class ReconciliationTests(unittest.TestCase):
    def test_reconciliation_detects_position_mismatch(self) -> None:
        report = reconcile_executor_state(
            trade_history_rows=[
                {
                    "leader_wallet": "wallet1",
                    "leader_user_name": "leader",
                    "category": "SPORTS",
                    "token_id": "tokenA",
                    "side": "BUY",
                    "event_type": "ENTRY",
                    "amount_usd": 2.0,
                    "price": 0.50,
                }
            ],
            open_position_rows=[],
            processed_signal_rows=[],
            order_attempt_rows=[],
        )

        self.assertEqual(report.summary["position_mismatches"], 1)
        self.assertEqual(report.issue_rows[0]["issue_type"], "POSITION_MISMATCH")

    def test_reconciliation_detects_stuck_signal_and_nonfinal_attempt(self) -> None:
        report = reconcile_executor_state(
            trade_history_rows=[],
            open_position_rows=[],
            processed_signal_rows=[
                {
                    "signal_id": "sig1",
                    "leader_wallet": "wallet1",
                    "token_id": "tokenA",
                    "status": "PROCESSING",
                    "reason": "signal claimed",
                }
            ],
            order_attempt_rows=[
                {
                    "attempt_id": 1,
                    "signal_id": "sig1",
                    "leader_wallet": "wallet1",
                    "token_id": "tokenA",
                    "status": "RISK_APPROVED",
                }
            ],
        )

        issue_types = {row["issue_type"] for row in report.issue_rows}
        self.assertIn("SIGNAL_STUCK_PROCESSING", issue_types)
        self.assertIn("ORDER_ATTEMPT_NOT_FINAL", issue_types)

    def test_recovered_live_fill_is_final_attempt_status(self) -> None:
        report = reconcile_executor_state(
            trade_history_rows=[],
            open_position_rows=[],
            processed_signal_rows=[],
            order_attempt_rows=[
                {
                    "attempt_id": 1,
                    "signal_id": "sig-recovered",
                    "leader_wallet": "wallet1",
                    "token_id": "tokenA",
                    "status": "LIVE_FILLED_RECOVERED",
                }
            ],
        )

        issue_types = {row["issue_type"] for row in report.issue_rows}
        self.assertNotIn("ORDER_ATTEMPT_NOT_FINAL", issue_types)

    def test_reviewed_live_submit_error_is_final_attempt_status(self) -> None:
        report = reconcile_executor_state(
            trade_history_rows=[],
            open_position_rows=[],
            processed_signal_rows=[],
            order_attempt_rows=[
                {
                    "attempt_id": 1,
                    "signal_id": "sig-reviewed",
                    "leader_wallet": "wallet1",
                    "token_id": "tokenA",
                    "status": "LIVE_SUBMIT_ERROR_REVIEWED",
                }
            ],
        )

        issue_types = {row["issue_type"] for row in report.issue_rows}
        self.assertNotIn("ORDER_ATTEMPT_NOT_FINAL", issue_types)

    def test_reconciliation_detects_filled_signal_without_attempt(self) -> None:
        report = reconcile_executor_state(
            trade_history_rows=[],
            open_position_rows=[],
            processed_signal_rows=[
                {
                    "signal_id": "sig2",
                    "leader_wallet": "wallet1",
                    "token_id": "tokenA",
                    "status": "PREVIEW_READY_ENTRY",
                    "reason": "ok",
                }
            ],
            order_attempt_rows=[],
        )

        self.assertEqual(report.issue_rows[0]["issue_type"], "FILLED_SIGNAL_WITHOUT_ORDER_ATTEMPT")

    def test_reconciliation_compares_aggregate_exchange_token_qty(self) -> None:
        report = reconcile_executor_state(
            trade_history_rows=[],
            open_position_rows=[
                {
                    "leader_wallet": "wallet1",
                    "token_id": "tokenA",
                    "position_usd": 2.0,
                    "avg_entry_price": 0.50,
                },
                {
                    "leader_wallet": "wallet2",
                    "token_id": "tokenA",
                    "position_usd": 1.0,
                    "avg_entry_price": 0.25,
                },
            ],
            processed_signal_rows=[],
            order_attempt_rows=[],
            exchange_position_rows=[
                {
                    "token_id": "tokenA",
                    "size": 7.0,
                    "current_value_usd": 3.5,
                }
            ],
        )

        issue_types = {row["issue_type"] for row in report.issue_rows}
        self.assertIn("EXCHANGE_POSITION_QTY_MISMATCH", issue_types)

    def test_reconciliation_skips_exchange_compare_when_not_requested(self) -> None:
        report = reconcile_executor_state(
            trade_history_rows=[],
            open_position_rows=[
                {
                    "leader_wallet": "wallet1",
                    "token_id": "tokenA",
                    "position_usd": 2.0,
                    "avg_entry_price": 0.50,
                }
            ],
            processed_signal_rows=[],
            order_attempt_rows=[],
        )

        issue_types = {row["issue_type"] for row in report.issue_rows}
        self.assertNotIn("LOCAL_POSITION_NOT_ON_EXCHANGE", issue_types)

    def test_reconciliation_flags_local_only_when_exchange_compare_requested(self) -> None:
        report = reconcile_executor_state(
            trade_history_rows=[],
            open_position_rows=[
                {
                    "leader_wallet": "wallet1",
                    "token_id": "tokenA",
                    "position_usd": 2.0,
                    "avg_entry_price": 0.50,
                }
            ],
            processed_signal_rows=[],
            order_attempt_rows=[],
            exchange_position_rows=[],
        )

        issue_types = {row["issue_type"] for row in report.issue_rows}
        self.assertIn("LOCAL_POSITION_NOT_ON_EXCHANGE", issue_types)

    def test_reconciliation_flags_exchange_open_order(self) -> None:
        report = reconcile_executor_state(
            trade_history_rows=[],
            open_position_rows=[],
            processed_signal_rows=[],
            order_attempt_rows=[],
            exchange_open_order_rows=[
                {
                    "order_id": "order1",
                    "token_id": "tokenA",
                    "side": "BUY",
                    "remaining_size": 2.0,
                }
            ],
        )

        self.assertEqual(report.summary["exchange_open_orders"], 1)

    def test_reconciliation_includes_external_fetch_issues(self) -> None:
        report = reconcile_executor_state(
            trade_history_rows=[],
            open_position_rows=[],
            processed_signal_rows=[],
            order_attempt_rows=[],
            external_issue_rows=[
                {
                    "issue_type": "EXCHANGE_FETCH_ERROR",
                    "severity": "WARN",
                    "details": "exchange position fetch failed",
                }
            ],
        )

        self.assertEqual(report.summary["exchange_fetch_issues"], 1)


if __name__ == "__main__":
    unittest.main()
