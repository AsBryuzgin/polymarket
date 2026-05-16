from __future__ import annotations

import unittest

from backtest.replay import replay_signal_observations
from backtest.simulator import simulate_position_fills


class BacktestReplayTests(unittest.TestCase):
    def test_signal_observation_replay_uses_only_replayable_statuses(self) -> None:
        observations = [
            {
                "observation_id": 1,
                "observed_at": "2026-01-01 00:00:00",
                "leader_wallet": "wallet1",
                "leader_user_name": "leader",
                "category": "SPORTS",
                "latest_status": "POLICY_BLOCKED",
                "selected_signal_id": "blocked-buy",
                "selected_side": "BUY",
                "token_id": "tokenA",
                "selected_trade_notional_usd": 20.0,
                "selected_leader_portfolio_value_usd": 100.0,
                "target_budget_usd": 10.0,
                "snapshot_midpoint": 0.50,
                "snapshot_best_bid": 0.49,
            },
            {
                "observation_id": 2,
                "observed_at": "2026-01-01 00:01:00",
                "leader_wallet": "wallet1",
                "leader_user_name": "leader",
                "category": "SPORTS",
                "latest_status": "FRESH_COPYABLE",
                "selected_signal_id": "valid-buy",
                "selected_side": "BUY",
                "token_id": "tokenA",
                "selected_trade_notional_usd": 20.0,
                "selected_leader_portfolio_value_usd": 100.0,
                "target_budget_usd": 10.0,
                "snapshot_midpoint": 0.50,
                "snapshot_best_bid": 0.49,
            },
        ]

        report = replay_signal_observations(
            observations,
            leader_trade_notional_copy_fraction=0.20,
            min_order_size_usd=1.0,
            max_per_trade_usd=10.0,
        )

        entries = [row for row in report.event_rows if row["replay_event_type"] == "ENTRY"]
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["selected_signal_id"], "valid-buy")
        self.assertEqual(entries[0]["amount_usd"], 2.0)
        self.assertEqual(report.skipped_rows[0]["skip_reason"], "latest_status not replayable")

    def test_simulator_handles_partial_exit_and_realized_pnl(self) -> None:
        result = simulate_position_fills(
            [
                {
                    "leader_wallet": "wallet1",
                    "leader_user_name": "leader",
                    "category": "CRYPTO",
                    "token_id": "tokenA",
                    "selected_signal_id": "buy",
                    "side": "BUY",
                    "amount_usd": 4.0,
                    "exec_price": 0.50,
                },
                {
                    "leader_wallet": "wallet1",
                    "leader_user_name": "leader",
                    "category": "CRYPTO",
                    "token_id": "tokenA",
                    "selected_signal_id": "sell",
                    "side": "SELL",
                    "amount_usd": 1.0,
                    "exec_price": 0.60,
                },
            ]
        )

        exits = [row for row in result.event_rows if row["replay_event_type"] == "EXIT"]
        final_rows = [row for row in result.event_rows if row["replay_event_type"] == "FINAL_STATE"]

        self.assertEqual(exits[0]["amount_usd"], 1.0)
        self.assertEqual(exits[0]["position_after_usd"], 3.0)
        self.assertEqual(exits[0]["realized_pnl_usd"], 0.2)
        self.assertEqual(final_rows[0]["position_after_usd"], 3.0)
        self.assertEqual(final_rows[0]["realized_pnl_usd"], 0.2)

    def test_signal_observation_replay_can_round_up_to_min_order(self) -> None:
        report = replay_signal_observations(
            [
                {
                    "observation_id": 1,
                    "observed_at": "2026-01-01 00:00:00",
                    "leader_wallet": "wallet1",
                    "leader_user_name": "leader",
                    "category": "ECONOMICS",
                    "latest_status": "FRESH_COPYABLE",
                    "selected_signal_id": "tiny-buy",
                    "selected_side": "BUY",
                    "token_id": "tokenA",
                    "selected_trade_notional_usd": 50.0,
                    "selected_leader_portfolio_value_usd": 1000.0,
                    "target_budget_usd": 12.0,
                    "snapshot_midpoint": 0.50,
                    "snapshot_best_bid": 0.49,
                }
            ],
            leader_trade_notional_copy_fraction=0.20,
            min_order_size_usd=1.85,
            max_per_trade_usd=10.0,
            round_up_to_min_order=True,
        )

        entries = [row for row in report.event_rows if row["replay_event_type"] == "ENTRY"]
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["amount_usd"], 1.85)


if __name__ == "__main__":
    unittest.main()
