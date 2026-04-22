from __future__ import annotations

import unittest

from execution.copy_worker import LeaderSignal
from execution.soak_runner import filter_registry_rows_for_scan, run_soak_cycle, summarize_soak_cycle


class SoakRunnerTests(unittest.TestCase):
    def test_filter_registry_rows_skips_exit_only_without_open_positions(self) -> None:
        rows = filter_registry_rows_for_scan(
            registry_rows=[
                {"wallet": "active", "leader_status": "ACTIVE"},
                {"wallet": "exit-flat", "leader_status": "EXIT_ONLY"},
                {"wallet": "exit-open", "leader_status": "EXIT_ONLY"},
            ],
            open_positions=[
                {"leader_wallet": "exit-open", "token_id": "tokenA", "position_usd": 1.0},
            ],
        )

        self.assertEqual([row["wallet"] for row in rows], ["active", "exit-open"])

    def test_soak_cycle_logs_observation_and_processes_selected_signal(self) -> None:
        logged_observations = []
        signal = LeaderSignal(
            signal_id="sig1",
            leader_wallet="wallet1",
            token_id="tokenA",
            side="BUY",
            leader_budget_usd=10.0,
            leader_trade_notional_usd=5.0,
        )

        def fetcher(*, wallet: str, leader_budget_usd: float):
            self.assertEqual(wallet, "wallet1")
            self.assertEqual(leader_budget_usd, 10.0)
            return (
                signal,
                {
                    "midpoint": 0.50,
                    "best_bid": 0.49,
                    "best_ask": 0.51,
                    "spread": 0.02,
                },
                {
                    "latest_trade_side": "BUY",
                    "latest_trade_age_sec": 4.0,
                    "latest_trade_hash": "sig1",
                    "latest_status": "FRESH_COPYABLE",
                    "latest_reason": "copyable",
                    "selected_trade_age_sec": 4.0,
                    "selected_trade_notional_usd": 5.0,
                },
            )

        def processor(selected_signal: LeaderSignal):
            self.assertEqual(selected_signal.signal_id, "sig1")
            return {"status": "PAPER_FILLED_ENTRY", "reason": "ok"}

        rows = run_soak_cycle(
            registry_rows=[
                {
                    "wallet": "wallet1",
                    "user_name": "leader",
                    "category": "SPORTS",
                    "leader_status": "ACTIVE",
                    "target_budget_usd": 10.0,
                }
            ],
            signal_fetcher=fetcher,
            signal_processor=processor,
            observation_logger=lambda **kwargs: logged_observations.append(kwargs),
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["process_status"], "PAPER_FILLED_ENTRY")
        self.assertEqual(rows[0]["selected_signal_id"], "sig1")
        self.assertEqual(len(logged_observations), 1)
        self.assertEqual(logged_observations[0]["latest_status"], "FRESH_COPYABLE")
        self.assertEqual(logged_observations[0]["selected_signal_id"], "sig1")

        summary = summarize_soak_cycle(rows)
        self.assertEqual(summary["leaders_checked"], 1)
        self.assertEqual(summary["selected_signals"], 1)
        self.assertEqual(summary["process_status_counts"]["PAPER_FILLED_ENTRY"], 1)

    def test_soak_cycle_logs_no_signal_observation_without_processing(self) -> None:
        logged_observations = []

        def fetcher(*, wallet: str, leader_budget_usd: float):
            return (
                None,
                None,
                {
                    "latest_trade_side": "SELL",
                    "latest_trade_age_sec": 30.0,
                    "latest_trade_hash": "sell1",
                    "latest_status": "SKIPPED_NO_POSITION",
                    "latest_reason": "sell signal but no copied open position",
                },
            )

        def processor(_signal: LeaderSignal):
            raise AssertionError("processor should not be called")

        rows = run_soak_cycle(
            registry_rows=[
                {
                    "wallet": "wallet2",
                    "user_name": "leader2",
                    "category": "CRYPTO",
                    "leader_status": "ACTIVE",
                    "target_budget_usd": 8.0,
                }
            ],
            signal_fetcher=fetcher,
            signal_processor=processor,
            observation_logger=lambda **kwargs: logged_observations.append(kwargs),
        )

        self.assertEqual(rows[0]["process_status"], "NO_SIGNAL")
        self.assertEqual(rows[0]["latest_status"], "SKIPPED_NO_POSITION")
        self.assertIsNone(logged_observations[0]["selected_signal_id"])

    def test_soak_cycle_records_source_errors_as_observations(self) -> None:
        logged_observations = []

        def fetcher(*, wallet: str, leader_budget_usd: float):
            raise RuntimeError("source down")

        rows = run_soak_cycle(
            registry_rows=[
                {
                    "wallet": "wallet3",
                    "user_name": "leader3",
                    "category": "FINANCE",
                    "leader_status": "ACTIVE",
                    "target_budget_usd": 7.0,
                }
            ],
            signal_fetcher=fetcher,
            observation_logger=lambda **kwargs: logged_observations.append(kwargs),
        )

        self.assertEqual(rows[0]["process_status"], "SOURCE_ERROR")
        self.assertEqual(logged_observations[0]["latest_status"], "SOURCE_ERROR")
        self.assertEqual(logged_observations[0]["latest_reason"], "source down")


if __name__ == "__main__":
    unittest.main()
