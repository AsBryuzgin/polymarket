from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import execution.state_store as state_store
from execution.copy_worker import LeaderSignal, process_signal
from execution.signal_observation_store import init_signal_observation_table, log_signal_observation
from risk.adaptive_sizing import compute_adaptive_sizing_decision


class AdaptiveSizingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        state_store.DB_PATH = Path(self.tmpdir.name) / "executor_state.db"
        state_store.init_db()
        init_signal_observation_table()

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def _observation(self, signal_id: str, *, notional: float = 30.0) -> dict:
        return {
            "observed_at": "2026-04-29 10:00:00",
            "leader_wallet": "wallet1",
            "selected_signal_id": signal_id,
            "selected_side": "BUY",
            "target_budget_usd": 20.0,
            "selected_trade_notional_usd": notional,
            "selected_leader_portfolio_value_usd": 100.0,
        }

    def test_historical_pressure_reduces_multiplier(self) -> None:
        config = {
            "sizing": {"max_leader_trade_budget_fraction": 0.25},
            "adaptive_sizing": {
                "enabled": True,
                "lookback_hours": 24.0,
                "target_budget_turnover": 0.85,
                "min_buy_signals_for_history": 5,
            },
        }

        decision = compute_adaptive_sizing_decision(
            leader_wallet="wallet1",
            leader_budget_usd=20.0,
            config=config,
            open_positions=[],
            observations=[self._observation(f"sig-{idx}") for idx in range(5)],
            processed_signals=[],
            trade_history=[],
            now=datetime(2026, 4, 29, 12, 0, tzinfo=timezone.utc),
        )

        self.assertLess(decision.historical_multiplier, 1.0)
        self.assertAlmostEqual(decision.multiplier, 0.68, places=2)
        self.assertEqual(decision.details["selected_buy_demand_usd"], 25.0)

    def test_live_utilization_reduces_multiplier(self) -> None:
        config = {
            "adaptive_sizing": {
                "enabled": True,
                "utilization_throttle_start": 0.60,
                "utilization_throttle_full": 0.90,
                "min_utilization_multiplier": 0.25,
            },
        }

        decision = compute_adaptive_sizing_decision(
            leader_wallet="wallet1",
            leader_budget_usd=20.0,
            config=config,
            open_positions=[{"leader_wallet": "wallet1", "position_usd": 18.0}],
            observations=[],
            processed_signals=[],
            trade_history=[],
            now=datetime(2026, 4, 29, 12, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(decision.utilization_multiplier, 0.25)
        self.assertEqual(decision.multiplier, 0.25)

    def test_budget_skip_pressure_reduces_multiplier(self) -> None:
        config = {
            "adaptive_sizing": {
                "enabled": True,
                "min_budget_skip_samples": 10,
                "budget_skip_ratio_start": 0.20,
                "min_budget_skip_multiplier": 0.25,
            },
        }
        processed = [
            {
                "leader_wallet": "wallet1",
                "side": "BUY",
                "status": "SKIPPED_RISK",
                "reason": "wallet exposure above leader budget",
                "created_at": "2026-04-29 10:00:00",
            }
            for _idx in range(8)
        ]
        history = [
            {
                "leader_wallet": "wallet1",
                "event_type": "ENTRY",
                "amount_usd": 1.0,
                "event_time": "2026-04-29 10:00:00",
            }
            for _idx in range(2)
        ]

        decision = compute_adaptive_sizing_decision(
            leader_wallet="wallet1",
            leader_budget_usd=20.0,
            config=config,
            open_positions=[],
            observations=[],
            processed_signals=processed,
            trade_history=history,
            now=datetime(2026, 4, 29, 12, 0, tzinfo=timezone.utc),
        )

        self.assertLess(decision.historical_multiplier, 1.0)
        self.assertEqual(decision.details["budget_skip_ratio"], 0.8)
        self.assertAlmostEqual(decision.multiplier, 0.4375)

    def test_copy_worker_applies_adaptive_multiplier_to_buy_size(self) -> None:
        log_signal_observation(
            leader_wallet="wallet1",
            leader_user_name="Leader",
            category="SPORTS",
            leader_status="ACTIVE",
            target_budget_usd=20.0,
            latest_trade_side="BUY",
            latest_trade_age_sec=1.0,
            latest_trade_hash="hash1",
            latest_status="FRESH_COPYABLE",
            latest_reason="ok",
            selected_signal_id="historical-sig",
            selected_side="BUY",
            token_id="tokenX",
            selected_trade_age_sec=1.0,
            selected_trade_notional_usd=50.0,
            selected_leader_portfolio_value_usd=100.0,
        )

        signal = LeaderSignal(
            signal_id="sig-buy-adaptive",
            leader_wallet="wallet1",
            token_id="tokenA",
            side="BUY",
            leader_budget_usd=20.0,
            leader_trade_notional_usd=10.0,
            leader_portfolio_value_usd=100.0,
        )
        config = {
            "risk": {
                "min_order_size_usd": 0.01,
                "max_per_trade_usd": 100.0,
                "skip_if_spread_gt": 0.02,
                "enforce_leader_budget_cap": True,
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
                "max_leader_trade_budget_fraction": 1.0,
            },
            "adaptive_sizing": {
                "enabled": True,
                "lookback_hours": 24.0,
                "target_budget_turnover": 0.25,
                "min_buy_signals_for_history": 1,
                "min_historical_multiplier": 0.10,
            },
        }
        snapshot = {
            "midpoint": 0.50,
            "spread": 0.01,
            "price_quote": 0.50,
            "best_bid": 0.49,
            "best_ask": 0.51,
        }

        with (
            patch("execution.copy_worker.load_executor_config", return_value=config),
            patch("execution.copy_worker.fetch_market_snapshot", return_value=snapshot),
            patch("execution.copy_worker.preview_market_order", return_value={"ok": True}),
        ):
            result = process_signal(signal)

        self.assertEqual(result["status"], "PREVIEW_READY_ENTRY")
        self.assertEqual(result["suggested_amount_usd"], 1.0)
        self.assertEqual(result["adaptive_sizing"]["multiplier"], 0.5)


if __name__ == "__main__":
    unittest.main()
