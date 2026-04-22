from __future__ import annotations

import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from unittest.mock import patch

from execution.telegram_reports import (
    build_activity_report,
    build_blocks_report,
    build_leaders_report,
    build_status_report,
)


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

        self.assertIn("cash excluding open: $100.00", report)
        self.assertIn("equity by bid: $106.00", report)
        self.assertIn("leaders: 1 active, 1 exit-only", report)

    def test_status_report_uses_paper_bankroll_when_configured(self) -> None:
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
            positions.return_value = [
                {
                    "leader_wallet": "wallet1",
                    "token_id": "tokenA",
                    "position_usd": 5.0,
                    "avg_entry_price": 0.50,
                }
            ]
            registry.return_value = []
            observations.return_value = []

            report = build_status_report(
                {
                    "global": {"execution_mode": "paper"},
                    "capital": {"total_capital_usd": 100.0},
                },
                snapshot_loader=snapshot_loader,
            )

        funding.assert_not_called()
        self.assertIn("paper bankroll: $100.00", report)
        self.assertIn("cash excluding open: $95.00", report)
        self.assertIn("equity by bid: $101.00", report)

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
                    "latest_trade_hash": "hash1",
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

        self.assertIn("observations: 1 | latest trades: 1 | selected unique: 1", report)
        self.assertIn("FRESH_COPYABLE: 1/1", report)
        self.assertIn("realized: +$0.25", report)
        self.assertIn("Leader", report)

    def test_leaders_report_handles_registry_map(self) -> None:
        with (
            patch("execution.telegram_reports.init_db"),
            patch("execution.telegram_reports.list_trade_history") as history,
            patch("execution.telegram_reports.list_open_positions") as positions,
            patch("execution.telegram_reports.list_leader_registry") as registry,
        ):
            history.return_value = [
                {
                    "leader_wallet": "wallet1",
                    "leader_user_name": "Leader",
                    "category": "ECONOMICS",
                    "event_type": "ENTRY",
                }
            ]
            positions.return_value = []
            registry.return_value = [
                {
                    "wallet": "wallet1",
                    "user_name": "Leader",
                    "category": "ECONOMICS",
                    "leader_status": "ACTIVE",
                }
            ]

            report = build_leaders_report(snapshot_loader=lambda _token_id, _side: {})

        self.assertIn("Leaders by bot PnL", report)
        self.assertIn("Leader", report)

    def test_blocks_report_shows_observation_and_unique_counts(self) -> None:
        now = datetime(2026, 4, 22, tzinfo=timezone.utc)
        with (
            patch("execution.telegram_reports.init_db"),
            patch("execution.telegram_reports.init_signal_observation_table"),
            patch("execution.telegram_reports.list_signal_observations") as observations,
        ):
            observations.return_value = [
                {
                    "observed_at": "2026-04-21 12:30:00",
                    "leader_wallet": "wallet1",
                    "leader_user_name": "Leader",
                    "category": "TECH",
                    "latest_status": "DRIFT_BLOCKED",
                    "latest_reason": "buy price drift abs too high: 0.0910 > 0.0100",
                    "latest_trade_hash": "hash1",
                },
                {
                    "observed_at": "2026-04-21 12:31:00",
                    "leader_wallet": "wallet1",
                    "leader_user_name": "Leader",
                    "category": "TECH",
                    "latest_status": "DRIFT_BLOCKED",
                    "latest_reason": "buy price drift abs too high: 0.0910 > 0.0100",
                    "latest_trade_hash": "hash1",
                },
            ]

            report = build_blocks_report(now=now)

        self.assertIn("DRIFT_BLOCKED: obs 2 | unique 1", report)
        self.assertIn("Leader", report)
        self.assertIn("2/1", report)


if __name__ == "__main__":
    unittest.main()
