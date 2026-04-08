from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import execution.state_store as state_store
from execution.copy_worker import LeaderSignal, process_signal


class ExecutionRegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "test_executor_state.db"
        state_store.DB_PATH = self.db_path
        state_store.init_db()

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_reentry_same_token_after_full_close(self) -> None:
        first = state_store.upsert_buy_position(
            leader_wallet="wallet1",
            token_id="tokenA",
            amount_usd=2.0,
            entry_price=0.40,
            signal_id="buy1",
        )
        self.assertEqual(first["position_after_usd"], 2.0)

        closed = state_store.reduce_or_close_position(
            leader_wallet="wallet1",
            token_id="tokenA",
            signal_id="sell1",
            amount_usd=2.0,
        )
        self.assertIsNotNone(closed)
        self.assertEqual(closed["position_after_usd"], 0.0)
        self.assertTrue(closed["closed_fully"])

        reopened = state_store.upsert_buy_position(
            leader_wallet="wallet1",
            token_id="tokenA",
            amount_usd=1.5,
            entry_price=0.55,
            signal_id="buy2",
        )
        self.assertEqual(reopened["position_before_usd"], 0.0)
        self.assertEqual(reopened["position_after_usd"], 1.5)
        self.assertEqual(reopened["entry_avg_price_after"], 0.55)

        pos = state_store.get_open_position("wallet1", "tokenA")
        self.assertIsNotNone(pos)
        self.assertEqual(float(pos["position_usd"]), 1.5)
        self.assertEqual(float(pos["avg_entry_price"]), 0.55)
        self.assertEqual(pos["status"], "OPEN")

    def test_buy_sizing_uses_leader_trade_notional(self) -> None:
        signal = LeaderSignal(
            signal_id="sig-buy-1",
            leader_wallet="wallet2",
            token_id="tokenB",
            side="BUY",
            leader_budget_usd=50.0,
            leader_trade_notional_usd=15.0,
        )

        config = {
            "risk": {
                "min_order_size_usd": 1.0,
                "max_per_trade_usd": 100.0,
                "skip_if_spread_gt": 0.02,
            },
            "filters": {
                "buy_min_price": 0.05,
                "buy_max_price": 0.95,
            },
            "exit": {
                "exit_max_spread": 0.05,
            },
            "sizing": {
                "leader_trade_notional_copy_fraction": 0.20,
            },
        }

        snapshot = {
            "midpoint": 0.50,
            "spread": 0.01,
            "price_quote": 0.50,
            "best_bid": 0.49,
            "best_ask": 0.51,
        }

        with patch("execution.copy_worker.load_executor_config", return_value=config), \
             patch("execution.copy_worker.fetch_market_snapshot", return_value=snapshot), \
             patch("execution.copy_worker.preview_market_order", return_value={"ok": True}):
            result = process_signal(signal)

        self.assertEqual(result["status"], "PREVIEW_READY_ENTRY")
        self.assertEqual(result["suggested_amount_usd"], 3.0)

        pos = state_store.get_open_position("wallet2", "tokenB")
        self.assertIsNotNone(pos)
        self.assertEqual(float(pos["position_usd"]), 3.0)

    def test_sell_partial_exit_reduces_position(self) -> None:
        state_store.upsert_buy_position(
            leader_wallet="wallet3",
            token_id="tokenC",
            amount_usd=2.0,
            entry_price=0.50,
            signal_id="seed-buy",
        )

        signal = LeaderSignal(
            signal_id="sig-sell-1",
            leader_wallet="wallet3",
            token_id="tokenC",
            side="SELL",
            leader_budget_usd=50.0,
            leader_trade_notional_usd=5.0,  # 20% => 1.0 sell
        )

        config = {
            "risk": {
                "min_order_size_usd": 1.0,
                "max_per_trade_usd": 100.0,
                "skip_if_spread_gt": 0.02,
            },
            "filters": {
                "buy_min_price": 0.05,
                "buy_max_price": 0.95,
            },
            "exit": {
                "exit_max_spread": 0.05,
            },
            "sizing": {
                "leader_trade_notional_copy_fraction": 0.20,
            },
        }

        snapshot = {
            "midpoint": 0.60,
            "spread": 0.01,
            "price_quote": 0.60,
            "best_bid": 0.59,
            "best_ask": 0.61,
        }

        with patch("execution.copy_worker.load_executor_config", return_value=config), \
             patch("execution.copy_worker.fetch_market_snapshot", return_value=snapshot), \
             patch("execution.copy_worker.preview_market_order", return_value={"ok": True}):
            result = process_signal(signal)

        self.assertEqual(result["status"], "PREVIEW_READY_PARTIAL_EXIT")
        self.assertEqual(result["suggested_amount_usd"], 1.0)
        self.assertEqual(result["position_after_usd"], 1.0)

        pos = state_store.get_open_position("wallet3", "tokenC")
        self.assertIsNotNone(pos)
        self.assertEqual(float(pos["position_usd"]), 1.0)


if __name__ == "__main__":
    unittest.main()
