from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import execution.state_store as state_store
from execution.copy_worker import LeaderSignal, process_signal
from execution.leader_signal_source import latest_fresh_copyable_signal_from_wallet


class SignalNotionalPassthroughTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "test_executor_state.db"
        state_store.DB_PATH = self.db_path
        state_store.init_db()

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_source_builds_signal_with_trade_notional_fields(self) -> None:
        fake_trades = [
            {
                "proxyWallet": "walletA",
                "side": "BUY",
                "asset": "tokenA",
                "conditionId": "condA",
                "size": 800.0,
                "price": 0.30525,
                "timestamp": 1700000000,
                "title": "title",
                "slug": "slug",
                "eventSlug": "event-slug",
                "outcome": "YES",
                "transactionHash": "txA",
            }
        ]

        fake_snapshot = {
            "best_ask": 0.282,
            "best_bid": 0.28,
            "midpoint": 0.281,
            "price_quote": 0.28,
            "spread": 0.002,
            "token_id": "tokenA",
        }

        fake_config = {
            "risk": {"skip_if_spread_gt": 0.02, "min_order_size_usd": 1.0},
            "filters": {"buy_min_price": 0.05, "buy_max_price": 0.95},
            "signal_freshness": {
                "preferred_signal_age_sec": 30,
                "max_buy_signal_age_sec": 10_000_000_000,
                "max_recent_trades": 3,
                "max_price_drift_abs": 1.0,
                "max_price_drift_rel": 1.0,
            },
            "exit": {"ignore_exit_drift": True, "exit_max_spread": 0.05},
        }

        with patch("execution.leader_signal_source.load_executor_config", return_value=fake_config), \
             patch("execution.leader_signal_source.WalletProfilesClient") as MockClient, \
             patch("execution.leader_signal_source.fetch_market_snapshot", return_value=fake_snapshot), \
             patch("execution.leader_signal_source.get_leader_registry", return_value={"leader_status": "ACTIVE"}):
            MockClient.return_value.get_trades.return_value = fake_trades
            MockClient.return_value.paginate_current_positions.return_value = [
                {
                    "asset": "tokenA",
                    "size": 800.0,
                    "currentValue": 1000.0,
                }
            ]

            signal, snapshot, summary = latest_fresh_copyable_signal_from_wallet(
                wallet="walletA",
                leader_budget_usd=12.84,
            )

        self.assertIsNotNone(signal)
        self.assertEqual(signal.leader_trade_size, 800.0)
        self.assertEqual(signal.leader_trade_price, 0.30525)
        self.assertAlmostEqual(signal.leader_trade_notional_usd, 244.2, places=6)
        self.assertAlmostEqual(signal.leader_portfolio_value_usd, 1000.0, places=6)
        self.assertAlmostEqual(summary["selected_trade_notional_usd"], 244.2, places=6)
        self.assertAlmostEqual(summary["selected_leader_portfolio_value_usd"], 1000.0, places=6)

    def test_source_blocks_stale_buy_signal(self) -> None:
        fake_trades = [
            {
                "proxyWallet": "walletA",
                "side": "BUY",
                "asset": "tokenA",
                "conditionId": "condA",
                "size": 10.0,
                "price": 0.50,
                "timestamp": 1700000000,
                "title": "title",
                "slug": "slug",
                "eventSlug": "event-slug",
                "outcome": "YES",
                "transactionHash": "tx-stale",
            }
        ]

        fake_config = {
            "risk": {"skip_if_spread_gt": 0.02, "min_order_size_usd": 1.0},
            "filters": {"buy_min_price": 0.05, "buy_max_price": 0.95},
            "signal_freshness": {
                "preferred_signal_age_sec": 30,
                "max_buy_signal_age_sec": 600,
                "max_recent_trades": 3,
                "max_price_drift_abs": 1.0,
                "max_price_drift_rel": 1.0,
            },
            "exit": {"ignore_exit_drift": True, "exit_max_spread": 0.05},
        }

        with patch("execution.leader_signal_source.load_executor_config", return_value=fake_config), \
             patch("execution.leader_signal_source.WalletProfilesClient") as MockClient, \
             patch("execution.leader_signal_source.get_leader_registry", return_value={"leader_status": "ACTIVE"}), \
             patch("execution.leader_signal_source.time.time", return_value=1700001001):
            MockClient.return_value.get_trades.return_value = fake_trades

            signal, snapshot, summary = latest_fresh_copyable_signal_from_wallet(
                wallet="walletA",
                leader_budget_usd=12.84,
            )

        self.assertIsNone(signal)
        self.assertIsNone(snapshot)
        self.assertEqual(summary["latest_status"], "TOO_OLD")
        self.assertIn("max_signal_age_sec 600s", summary["latest_reason"])

    def test_process_signal_uses_notional_sizing_not_fallback(self) -> None:
        signal = LeaderSignal(
            signal_id="sig-1",
            leader_wallet="walletB",
            token_id="tokenB",
            side="BUY",
            leader_budget_usd=12.84,
            leader_trade_size=800.0,
            leader_trade_price=0.30525,
            leader_trade_notional_usd=244.2,
            leader_portfolio_value_usd=1000.0,
        )

        fake_config = {
            "risk": {
                "min_order_size_usd": 1.0,
                "max_per_trade_usd": 2.0,
                "skip_if_spread_gt": 0.02,
            },
            "filters": {"buy_min_price": 0.05, "buy_max_price": 0.95},
            "exit": {"exit_max_spread": 0.05},
            "sizing": {"leader_trade_notional_copy_fraction": 0.20},
            "signal_freshness": {
                "max_price_drift_abs": 1.0,
                "max_price_drift_rel": 1.0,
            },
        }

        fake_snapshot = {
            "midpoint": 0.50,
            "spread": 0.01,
            "price_quote": 0.50,
            "best_bid": 0.49,
            "best_ask": 0.51,
        }

        with patch("execution.copy_worker.load_executor_config", return_value=fake_config), \
             patch("execution.copy_worker.fetch_market_snapshot", return_value=fake_snapshot), \
             patch("execution.copy_worker.preview_market_order", return_value={"ok": True}), \
             patch("execution.copy_worker.get_leader_registry", return_value=None):
            result = process_signal(signal)

        self.assertEqual(result["status"], "PREVIEW_READY_ENTRY")
        self.assertEqual(result["suggested_amount_usd"], 2.0)

        pos = state_store.get_open_position("walletB", "tokenB")
        self.assertIsNotNone(pos)
        self.assertEqual(float(pos["position_usd"]), 2.0)


if __name__ == "__main__":
    unittest.main()
