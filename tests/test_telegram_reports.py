from __future__ import annotations

import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from unittest.mock import patch

from execution.telegram_reports import build_activity_report, build_status_report


@dataclass
class FakeFunding:
    balance_usd: float
    allowance_usd: float


class TelegramReportTests(unittest.TestCase):
    def test_status_report_includes_cash_and_marked_equity(self) -> None:
        def snapshot_loader(_token_id: str, _side: str):
            return {"best_bid": 0.60, "midpoint": 0.65}

        with (
            patch("execution.telegram_reports.init_db"),
            patch("execution.telegram_reports.init_signal_observation_table"),
            patch("execution.telegram_reports.fetch_collateral_balance_allowance") as funding,
            patch("execution.telegram_reports.list_open_positions") as positions,
            patch("execution.telegram_reports.list_leader_registry") as registry,
            patch("execution.telegram_reports.list_signal_observations") as observations,
            patch("execution.telegram_reports._load_latest_alert_count", return_value=0),
        ):
            funding.return_value = FakeFunding(balance_usd=100.0, allowance_usd=25.0)
            positions.return_value = [
                {
                    "leader_wallet": "wallet1",
                    "token_id": "tokenA",
                    "position_usd": 5.0,
                    "avg_entry_price": 0.50,
                }
            ]
            registry.return_value = [
                {"wallet": "wallet1", "leader_status": "ACTIVE"},
                {"wallet": "wallet2", "leader_status": "EXIT_ONLY"},
            ]
            observations.return_value = []

            report = build_status_report(
                {"global": {"execution_mode": "paper"}},
                snapshot_loader=snapshot_loader,
            )

        self.assertIn("cash balance: $100.00", report)
        self.assertIn("equity by bid: $106.00", report)
        self.assertIn("leaders: 1 active, 1 exit-only", report)

    def test_activity_report_counts_last_day_observations(self) -> None:
        now = datetime(2026, 4, 22, tzinfo=timezone.utc)
        with (
            patch("execution.telegram_reports.init_db"),
            patch("execution.telegram_reports.init_signal_observation_table"),
            patch("execution.telegram_reports.list_signal_observations") as observations,
            patch("execution.telegram_reports.list_trade_history") as history,
        ):
            observations.return_value = [
                {
                    "observed_at": "2026-04-21 12:30:00",
                    "leader_wallet": "wallet1",
                    "leader_user_name": "Leader",
                    "category": "CRYPTO",
                    "latest_status": "FRESH_COPYABLE",
                    "selected_signal_id": "sig1",
                }
            ]
            history.return_value = [
                {
                    "event_time": "2026-04-21 13:00:00",
                    "event_type": "EXIT",
                    "realized_pnl_usd": 0.25,
                }
            ]

            report = build_activity_report(now=now)

        self.assertIn("observations: 1 | selected: 1", report)
        self.assertIn("realized: +$0.25", report)
        self.assertIn("Leader", report)


if __name__ == "__main__":
    unittest.main()
