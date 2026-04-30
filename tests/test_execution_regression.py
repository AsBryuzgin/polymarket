from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import execution.state_store as state_store
from execution.copy_worker import LeaderSignal, process_signal
from execution.order_router import OrderExecutionResult


class ExecutionRegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "test_executor_state.db"
        self.runtime_lock_path = Path(self.tmpdir.name) / "runtime.lock"
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

    def test_buy_sizing_uses_leader_trade_budget_fraction(self) -> None:
        signal = LeaderSignal(
            signal_id="sig-buy-1",
            leader_wallet="wallet2",
            token_id="tokenB",
            side="BUY",
            leader_budget_usd=50.0,
            leader_trade_notional_usd=15.0,
            leader_portfolio_value_usd=250.0,
        )

        config = {
            "risk": {
                "min_order_size_usd": 0.01,
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
            "best_ask": 0.50,
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

    def test_buy_uses_configured_usd_minimum_not_orderbook_share_minimum(self) -> None:
        signal = LeaderSignal(
            signal_id="sig-dynamic-min-order",
            leader_wallet="wallet-min",
            token_id="token-min",
            side="BUY",
            leader_budget_usd=20.0,
            leader_trade_notional_usd=5.0,
            leader_portfolio_value_usd=100.0,
            leader_trade_price=0.65,
        )
        config = {
            "risk": {
                "min_order_size_usd": 1.0,
                "max_per_trade_usd": 5.0,
                "skip_if_spread_gt": 0.03,
                "enforce_leader_budget_cap": True,
            },
            "filters": {
                "buy_min_price": 0.01,
                "buy_max_price": 0.96,
            },
            "exit": {
                "exit_max_spread": 0.05,
            },
            "sizing": {
                "leader_trade_notional_copy_fraction": 0.20,
                "round_up_to_min_order": True,
                "max_min_order_round_up_multiple": 3.0,
            },
            "signal_freshness": {
                "max_price_drift_abs": 1.0,
                "max_price_drift_rel": 1.0,
            },
        }
        snapshot = {
            "side": "BUY",
            "midpoint": 0.645,
            "spread": 0.01,
            "price_quote": 0.65,
            "best_bid": 0.64,
            "best_ask": 0.65,
            "min_order_size": 5.0,
        }

        with patch("execution.copy_worker.load_executor_config", return_value=config), \
             patch("execution.copy_worker.fetch_market_snapshot", return_value=snapshot), \
             patch("execution.copy_worker.preview_market_order", return_value={"ok": True}), \
             patch("execution.copy_worker.get_leader_registry", return_value=None):
            result = process_signal(signal)

        self.assertEqual(result["status"], "PREVIEW_READY_ENTRY")
        self.assertEqual(result["suggested_amount_usd"], 1.0)
        self.assertEqual(result["sizing"]["reason"], "ok")
        self.assertEqual(result["sizing"]["details"]["min_order_size_usd"], 1.0)

    def test_micro_buy_signals_accumulate_before_entry(self) -> None:
        config = {
            "risk": {
                "min_order_size_usd": 1.0,
                "max_per_trade_usd": 10.0,
                "skip_if_spread_gt": 0.03,
                "enforce_leader_budget_cap": True,
            },
            "filters": {
                "buy_min_price": 0.01,
                "buy_max_price": 0.96,
            },
            "exit": {
                "exit_max_spread": 0.05,
            },
            "sizing": {
                "leader_trade_notional_copy_fraction": 0.20,
                "round_up_to_min_order": True,
                "max_min_order_round_up_multiple": 3.0,
            },
            "micro_signal_accumulator": {
                "enabled": True,
                "max_bucket_age_sec": 600,
            },
            "signal_freshness": {
                "max_price_drift_abs": 1.0,
                "max_price_drift_rel": 1.0,
            },
        }
        snapshot = {
            "side": "BUY",
            "midpoint": 0.36,
            "spread": 0.01,
            "price_quote": 0.36,
            "best_bid": 0.355,
            "best_ask": 0.365,
            "min_order_size": 5.0,
        }

        def signal(signal_id: str) -> LeaderSignal:
            return LeaderSignal(
                signal_id=signal_id,
                leader_wallet="wallet-micro",
                token_id="token-micro",
                side="BUY",
                leader_budget_usd=20.0,
                leader_trade_notional_usd=2.0,
                leader_portfolio_value_usd=100.0,
                leader_trade_price=0.36,
            )

        with patch("execution.copy_worker.load_executor_config", return_value=config), \
             patch("execution.copy_worker.fetch_market_snapshot", return_value=snapshot), \
             patch("execution.copy_worker.preview_market_order", return_value={"ok": True}) as preview, \
             patch("execution.copy_worker.get_leader_registry", return_value=None):
            first = process_signal(signal("sig-micro-1"))
            second = process_signal(signal("sig-micro-2"))
            third = process_signal(signal("sig-micro-3"))

        self.assertEqual(first["status"], "ACCUMULATED_PENDING")
        self.assertEqual(second["status"], "ACCUMULATED_PENDING")
        self.assertEqual(third["status"], "PREVIEW_READY_ENTRY")
        self.assertAlmostEqual(third["suggested_amount_usd"], 1.2)
        self.assertEqual(preview.call_count, 1)

        statuses = {row["signal_id"]: row["status"] for row in state_store.list_processed_signals()}
        self.assertEqual(statuses["sig-micro-1"], "ACCUMULATED_EXECUTED")
        self.assertEqual(statuses["sig-micro-2"], "ACCUMULATED_EXECUTED")
        self.assertEqual(statuses["sig-micro-3"], "PREVIEW_READY_ENTRY")
        self.assertEqual(state_store.list_micro_signal_buckets(), [])

    def test_expired_micro_bucket_marks_pending_signals(self) -> None:
        state_store.accumulate_micro_signal(
            leader_wallet="wallet-micro",
            token_id="token-micro",
            side="BUY",
            signal_id="sig-expired-micro",
            amount_usd=0.40,
            max_age_sec=600,
        )
        state_store.record_signal(
            signal_id="sig-expired-micro",
            leader_wallet="wallet-micro",
            token_id="token-micro",
            side="BUY",
            leader_budget_usd=20.0,
            suggested_amount_usd=0.40,
            status="ACCUMULATED_PENDING",
            reason="waiting for min order",
        )
        conn = state_store.get_connection()
        conn.execute(
            """
            UPDATE micro_signal_buckets
            SET updated_at = '2000-01-01 00:00:00'
            WHERE leader_wallet = 'wallet-micro'
              AND token_id = 'token-micro'
              AND side = 'BUY'
            """
        )
        conn.commit()
        conn.close()

        reset_bucket = state_store.accumulate_micro_signal(
            leader_wallet="wallet-micro",
            token_id="token-micro",
            side="BUY",
            signal_id="sig-new-micro",
            amount_usd=0.40,
            max_age_sec=1,
        )

        statuses = {row["signal_id"]: row["status"] for row in state_store.list_processed_signals()}
        self.assertTrue(reset_bucket["reset"])
        self.assertEqual(reset_bucket["expired_signal_ids"], ["sig-expired-micro"])
        self.assertEqual(statuses["sig-expired-micro"], "ACCUMULATED_EXPIRED")
        self.assertAlmostEqual(state_store.list_micro_signal_buckets()[0]["pending_amount_usd"], 0.40)

    def test_duplicate_signal_is_claimed_before_preview(self) -> None:
        signal = LeaderSignal(
            signal_id="sig-duplicate-claim",
            leader_wallet="wallet-dup",
            token_id="tokenD",
            side="BUY",
            leader_budget_usd=50.0,
            leader_trade_notional_usd=10.0,
            leader_portfolio_value_usd=100.0,
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
            "best_ask": 0.50,
        }

        with patch("execution.copy_worker.load_executor_config", return_value=config), \
             patch("execution.copy_worker.fetch_market_snapshot", return_value=snapshot), \
             patch("execution.copy_worker.preview_market_order", return_value={"ok": True}) as preview:
            first = process_signal(signal)
            second = process_signal(signal)

        self.assertEqual(first["status"], "PREVIEW_READY_ENTRY")
        self.assertEqual(second["status"], "DUPLICATE")
        self.assertEqual(preview.call_count, 1)

    def test_buy_blocks_on_entry_price_drift_not_snapshot_age(self) -> None:
        signal = LeaderSignal(
            signal_id="sig-buy-drift-block",
            leader_wallet="wallet-drift",
            token_id="tokenE",
            side="BUY",
            leader_budget_usd=50.0,
            leader_trade_price=0.50,
            leader_trade_notional_usd=10.0,
            leader_portfolio_value_usd=100.0,
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
            "signal_freshness": {
                "max_price_drift_abs": 0.01,
                "max_price_drift_rel": 0.02,
            },
        }

        snapshot = {
            "midpoint": 0.54,
            "spread": 0.01,
            "price_quote": 0.54,
            "best_bid": 0.535,
            "best_ask": 0.545,
        }

        with patch("execution.copy_worker.load_executor_config", return_value=config), \
             patch("execution.copy_worker.fetch_market_snapshot", return_value=snapshot), \
             patch("execution.copy_worker.preview_market_order") as preview:
            result = process_signal(signal)

        preview.assert_not_called()
        self.assertEqual(result["status"], "SKIPPED_DRIFT")
        self.assertIn("buy price drift", result["reason"])
        self.assertIsNone(state_store.get_open_position("wallet-drift", "tokenE"))
        self.assertEqual(state_store.list_order_attempts("sig-buy-drift-block"), [])

    def test_live_mode_without_ack_is_blocked_before_position_update(self) -> None:
        signal = LeaderSignal(
            signal_id="sig-live-blocked",
            leader_wallet="wallet-live",
            token_id="tokenF",
            side="BUY",
            leader_budget_usd=50.0,
            leader_trade_price=0.50,
            leader_trade_notional_usd=10.0,
            leader_portfolio_value_usd=100.0,
        )

        config = {
            "global": {
                "simulation": False,
                "preview_mode": False,
                "execution_mode": "live",
                "live_trading_enabled": False,
                "live_trading_ack": "",
            },
            "runtime_lock": {
                "enabled": True,
                "path": str(self.runtime_lock_path),
            },
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
            "signal_freshness": {
                "max_price_drift_abs": 0.01,
                "max_price_drift_rel": 0.02,
            },
        }

        snapshot = {
            "midpoint": 0.50,
            "spread": 0.01,
            "price_quote": 0.50,
            "best_bid": 0.49,
            "best_ask": 0.50,
        }

        with patch("execution.copy_worker.load_executor_config", return_value=config), \
             patch("execution.copy_worker.fetch_market_snapshot", return_value=snapshot), \
             patch("execution.copy_worker.preview_market_order") as preview:
            result = process_signal(signal)

        preview.assert_not_called()
        self.assertEqual(result["status"], "SKIPPED_EXECUTION")
        self.assertIn("live trading disabled", result["reason"])
        self.assertIsNone(state_store.get_open_position("wallet-live", "tokenF"))

        attempts = state_store.list_order_attempts("sig-live-blocked")
        self.assertEqual(len(attempts), 1)
        self.assertEqual(attempts[0]["mode"], "LIVE")
        self.assertEqual(attempts[0]["status"], "LIVE_BLOCKED")

    def test_execution_error_does_not_create_position(self) -> None:
        signal = LeaderSignal(
            signal_id="sig-preview-error",
            leader_wallet="wallet-error",
            token_id="tokenG",
            side="BUY",
            leader_budget_usd=50.0,
            leader_trade_price=0.50,
            leader_trade_notional_usd=10.0,
            leader_portfolio_value_usd=100.0,
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
            "signal_freshness": {
                "max_price_drift_abs": 0.01,
                "max_price_drift_rel": 0.02,
            },
        }

        snapshot = {
            "midpoint": 0.50,
            "spread": 0.01,
            "price_quote": 0.50,
            "best_bid": 0.49,
            "best_ask": 0.50,
        }

        with patch("execution.copy_worker.load_executor_config", return_value=config), \
             patch("execution.copy_worker.fetch_market_snapshot", return_value=snapshot), \
             patch("execution.copy_worker.preview_market_order", side_effect=RuntimeError("preview failed")):
            result = process_signal(signal)

        self.assertEqual(result["status"], "EXECUTION_ERROR")
        self.assertIn("preview failed", result["reason"])
        self.assertIsNone(state_store.get_open_position("wallet-error", "tokenG"))

        attempts = state_store.list_order_attempts("sig-preview-error")
        self.assertEqual(len(attempts), 1)
        self.assertEqual(attempts[0]["status"], "EXECUTION_ERROR")

    def test_live_fill_updates_position_from_verified_fill_amount(self) -> None:
        signal = LeaderSignal(
            signal_id="sig-live-filled",
            leader_wallet="wallet-live-fill",
            token_id="tokenH",
            side="BUY",
            leader_budget_usd=50.0,
            leader_trade_price=0.50,
            leader_trade_notional_usd=10.0,
            leader_portfolio_value_usd=400.0,
        )

        config = {
            "global": {
                "simulation": False,
                "preview_mode": False,
                "execution_mode": "live",
                "live_trading_enabled": True,
                "live_trading_ack": "I_UNDERSTAND_THIS_PLACES_REAL_ORDERS",
            },
            "runtime_lock": {
                "enabled": True,
                "path": str(self.runtime_lock_path),
            },
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
            "signal_freshness": {
                "max_price_drift_abs": 0.01,
                "max_price_drift_rel": 0.02,
            },
        }

        snapshot = {
            "midpoint": 0.50,
            "spread": 0.01,
            "price_quote": 0.50,
            "best_bid": 0.49,
            "best_ask": 0.50,
        }

        with patch("execution.copy_worker.load_executor_config", return_value=config), \
             patch("execution.copy_worker.fetch_market_snapshot", return_value=snapshot), \
             patch(
                 "execution.copy_worker.execute_market_order",
                 return_value=OrderExecutionResult(
                     accepted=True,
                     mode="LIVE",
                     status="LIVE_FILLED",
                     reason="live order fill verified",
                     raw_response={"orderID": "order1", "filled_amount_usd": "1.25"},
                     fill_amount_usd=1.25,
                     order_id="order1",
                     details={"fill_price": 0.52},
                 ),
             ):
            result = process_signal(signal)

        self.assertEqual(result["status"], "LIVE_FILLED_ENTRY")
        self.assertEqual(result["suggested_amount_usd"], 1.25)

        pos = state_store.get_open_position("wallet-live-fill", "tokenH")
        self.assertIsNotNone(pos)
        self.assertEqual(float(pos["position_usd"]), 1.25)
        self.assertEqual(float(pos["avg_entry_price"]), 0.52)

        attempts = state_store.list_order_attempts("sig-live-filled")
        self.assertEqual(len(attempts), 1)
        self.assertEqual(attempts[0]["mode"], "LIVE")
        self.assertEqual(attempts[0]["status"], "LIVE_FILLED")
        self.assertEqual(attempts[0]["order_id"], "order1")
        self.assertEqual(float(attempts[0]["fill_amount_usd"]), 1.25)

    def test_live_stop_buy_blocks_new_buy_before_market_fetch(self) -> None:
        state_store.create_order_attempt(
            signal_id="sig-old-unknown",
            leader_wallet="wallet-live-fill",
            token_id="tokenH",
            side="BUY",
            amount_usd=2.0,
            mode="LIVE",
            status="LIVE_SUBMITTED_UNVERIFIED",
            reason="submitted but fill amount was not verified",
        )

        signal = LeaderSignal(
            signal_id="sig-live-safety-block",
            leader_wallet="wallet-live-fill",
            token_id="tokenI",
            side="BUY",
            leader_budget_usd=50.0,
            leader_trade_price=0.50,
            leader_trade_notional_usd=10.0,
            leader_portfolio_value_usd=100.0,
        )

        config = {
            "global": {
                "simulation": False,
                "preview_mode": False,
                "execution_mode": "live",
                "live_trading_enabled": True,
                "live_trading_ack": "I_UNDERSTAND_THIS_PLACES_REAL_ORDERS",
            },
            "live_safety": {
                "enable_stop_buy_on_critical": True,
            },
            "runtime_lock": {
                "enabled": True,
                "path": str(self.runtime_lock_path),
            },
        }

        with patch("execution.copy_worker.load_executor_config", return_value=config), \
             patch("execution.copy_worker.fetch_market_snapshot") as fetch_snapshot:
            result = process_signal(signal)

        fetch_snapshot.assert_not_called()
        self.assertEqual(result["status"], "SKIPPED_LIVE_SAFETY")
        self.assertIn("live stop-buy active", result["reason"])
        self.assertEqual(state_store.list_order_attempts("sig-live-safety-block"), [])

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
            leader_trade_notional_usd=5.0,
            leader_exit_fraction=0.5,
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
