from __future__ import annotations

import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from unittest.mock import patch

from execution.telegram_reports import (
    build_activity_report,
    build_blocks_report,
    build_leaders_report,
    build_settlements_report,
    build_status_report,
    build_unmarked_report,
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

        self.assertIn("свободно без открытых позиций: $100.00", report)
        self.assertIn("equity по bid: $106.00", report)
        self.assertIn("лидеры: 1 active, 1 exit-only", report)

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
        self.assertIn("банкролл paper: $100.00", report)
        self.assertIn("свободно без открытых позиций: $95.00", report)
        self.assertIn("equity по bid: $101.00", report)

    def test_status_report_separates_unmarked_snapshot_errors(self) -> None:
        def snapshot_loader(token_id: str, _side: str):
            if token_id == "tokenB":
                raise RuntimeError("No orderbook exists for the requested token id")
            return {"best_bid": 0.60, "midpoint": 0.65}

        with (
            patch("execution.telegram_reports.init_db"),
            patch("execution.telegram_reports.init_signal_observation_table"),
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
                },
                {
                    "leader_wallet": "wallet2",
                    "token_id": "tokenB",
                    "position_usd": 5.0,
                    "avg_entry_price": 0.50,
                },
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

        self.assertIn("open PnL по bid/mid: +$1.00 / +$1.50", report)
        self.assertIn("неоцененные по рынку: 1 | сумма: $5.00", report)
        self.assertIn("детали по неоцененным: /unmarked", report)

    def test_status_report_counts_settlement_fallback_as_marked(self) -> None:
        def snapshot_loader(token_id: str, _side: str):
            if token_id == "tokenB":
                raise RuntimeError("No orderbook exists for the requested token id")
            return {"best_bid": 0.60, "midpoint": 0.65}

        with (
            patch("execution.telegram_reports.init_db"),
            patch("execution.telegram_reports.init_signal_observation_table"),
            patch("execution.telegram_reports.list_open_positions") as positions,
            patch("execution.telegram_reports.list_leader_registry") as registry,
            patch("execution.telegram_reports.list_signal_observations") as observations,
            patch("execution.telegram_reports._load_latest_alert_count", return_value=0),
            patch(
                "execution.telegram_reports.diagnose_market_snapshot_error",
                return_value={
                    "diagnosis_status": "NO_ORDERBOOK_CLOSED_OR_RESOLVED",
                    "diagnosis_label": "closed/resolved",
                    "diagnosis_reason": "market resolved",
                    "action_hint": "redeem path is needed",
                    "token_winner": True,
                    "token_outcome": "Yes",
                },
            ),
        ):
            positions.return_value = [
                {
                    "leader_wallet": "wallet1",
                    "token_id": "tokenA",
                    "position_usd": 5.0,
                    "avg_entry_price": 0.50,
                },
                {
                    "leader_wallet": "wallet2",
                    "token_id": "tokenB",
                    "position_usd": 5.0,
                    "avg_entry_price": 0.50,
                },
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

        self.assertIn("equity по bid: $106.00", report)
        self.assertIn("settlement-marked: 1 | сумма: $5.00", report)
        self.assertNotIn("неоцененные по рынку: 1", report)

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
                    "leader_wallet": "wallet1",
                    "leader_user_name": "Leader",
                    "category": "CRYPTO",
                    "event_type": "EXIT",
                    "realized_pnl_usd": 0.25,
                }
            ]

            report = build_activity_report(now=now)

        self.assertIn("проверки: 1 | уникальные latest-сделки: 1", report)
        self.assertIn("выбранные сигналы: 1 проверок", report)
        self.assertIn("FRESH_COPYABLE: 1", report)
        self.assertIn("realized +$0.25", report)
        self.assertIn("Leader", report)
        self.assertIn("BUY 0 | SELL 1", report)

    def test_settlements_report_uses_settlement_builder(self) -> None:
        with (
            patch("execution.telegram_reports.init_db"),
            patch(
                "execution.telegram_reports.build_settlement_report",
                return_value="settlement report body",
            ),
        ):
            report = build_settlements_report({"global": {"execution_mode": "paper"}})

        self.assertEqual(report, "settlement report body")

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

        self.assertIn("Лидеры по PnL бота", report)
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
                    "latest_trade_side": "BUY",
                    "latest_trade_age_sec": 77.0,
                    "latest_token_id": "token1",
                    "latest_trade_price": 0.31,
                    "latest_snapshot_midpoint": 0.40,
                    "latest_snapshot_spread": 0.03,
                },
                {
                    "observed_at": "2026-04-21 12:31:00",
                    "leader_wallet": "wallet1",
                    "leader_user_name": "Leader",
                    "category": "TECH",
                    "latest_status": "DRIFT_BLOCKED",
                    "latest_reason": "buy price drift abs too high: 0.0910 > 0.0100",
                    "latest_trade_hash": "hash1",
                    "latest_trade_side": "BUY",
                    "latest_trade_age_sec": 137.0,
                    "latest_token_id": "token1",
                    "latest_trade_price": 0.31,
                    "latest_snapshot_midpoint": 0.41,
                    "latest_snapshot_spread": 0.03,
                },
            ]

            report = build_blocks_report(now=now)

        self.assertIn("DRIFT_BLOCKED: 2 проверок / 1 unique", report)
        self.assertIn("Leader", report)
        self.assertIn("price drift: 1 unique / 2 checks", report)
        self.assertIn("age 77s->2.3m", report)
        self.assertIn("leader px 0.3100", report)
        self.assertIn("mid 0.4100", report)
        self.assertIn("spread 0.0300 (7.3%)", report)

    def test_blocks_report_parses_old_spread_reason_when_snapshot_missing(self) -> None:
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
                    "category": "CULTURE",
                    "latest_status": "POLICY_BLOCKED",
                    "latest_reason": "spread 0.0400 (16.67% of midpoint) above max_allowed_spread 0.0300",
                    "latest_trade_hash": "hash1",
                    "latest_trade_side": "BUY",
                    "latest_trade_age_sec": 600.0,
                },
            ]

            report = build_blocks_report(now=now)

        self.assertIn("spread: 1 unique / 1 checks", report)
        self.assertIn("mid 0.2400", report)
        self.assertIn("spread 0.0400 (16.7%)", report)

    def test_unmarked_report_includes_market_diagnosis(self) -> None:
        def snapshot_loader(token_id: str, _side: str):
            if token_id == "tokenB":
                raise RuntimeError("No orderbook exists for the requested token id")
            return {"best_bid": 0.60, "midpoint": 0.65}

        with (
            patch("execution.telegram_reports.init_db"),
            patch("execution.telegram_reports.list_open_positions") as positions,
            patch("execution.telegram_reports.list_leader_registry") as registry,
            patch(
                "execution.telegram_reports.diagnose_market_snapshot_error",
                return_value={
                    "diagnosis_status": "NO_ORDERBOOK_DISABLED",
                    "diagnosis_label": "orderbook disabled",
                    "diagnosis_reason": "market exists but enableOrderBook=false",
                    "question": "Will it rain?",
                    "active": False,
                    "closed": True,
                    "archived": False,
                    "accepting_orders": False,
                    "enable_order_book": False,
                    "action_hint": "do not expect CLOB quotes for this token",
                },
            ),
        ):
            positions.return_value = [
                {
                    "leader_wallet": "wallet1",
                    "token_id": "tokenB",
                    "position_usd": 5.0,
                    "avg_entry_price": 0.50,
                }
            ]
            registry.return_value = [
                {
                    "wallet": "wallet1",
                    "user_name": "Leader",
                    "category": "CULTURE",
                }
            ]

            report = build_unmarked_report(snapshot_loader=snapshot_loader)

        self.assertIn("Неоцененные позиции", report)
        self.assertIn("Leader | CULTURE | $5.00", report)
        self.assertIn("orderbook disabled", report)
        self.assertIn("Will it rain?", report)


if __name__ == "__main__":
    unittest.main()
