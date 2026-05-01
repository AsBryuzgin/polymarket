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

    def _observation(
        self,
        signal_id: str,
        *,
        notional: float = 30.0,
        target_budget_usd: float = 20.0,
        portfolio_value_usd: float = 100.0,
        token_id: str = "tokenA",
        observed_at: str = "2026-04-29 10:00:00",
    ) -> dict:
        return {
            "observed_at": observed_at,
            "leader_wallet": "wallet1",
            "selected_signal_id": signal_id,
            "selected_side": "BUY",
            "token_id": token_id,
            "target_budget_usd": target_budget_usd,
            "selected_trade_notional_usd": notional,
            "selected_leader_portfolio_value_usd": portfolio_value_usd,
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
        self.assertEqual(decision.details["selected_buy_raw_demand_usd"], 25.0)

    def test_historical_pressure_accounts_for_min_order_round_up(self) -> None:
        config = {
            "risk": {
                "min_order_size_usd": 1.0,
                "max_per_trade_usd": 100.0,
            },
            "sizing": {
                "max_leader_trade_budget_fraction": 1.0,
                "round_up_to_min_order": True,
                "max_min_order_round_up_multiple": 3.0,
            },
            "adaptive_sizing": {
                "enabled": True,
                "lookback_hours": 24.0,
                "target_budget_turnover": 0.20,
                "min_buy_signals_for_history": 5,
                "min_historical_multiplier": 0.10,
            },
        }

        decision = compute_adaptive_sizing_decision(
            leader_wallet="wallet1",
            leader_budget_usd=20.0,
            config=config,
            open_positions=[],
            observations=[self._observation(f"sig-{idx}", notional=2.0) for idx in range(5)],
            processed_signals=[],
            trade_history=[],
            now=datetime(2026, 4, 29, 12, 0, tzinfo=timezone.utc),
        )

        self.assertAlmostEqual(decision.multiplier, 0.8)
        self.assertEqual(decision.details["selected_buy_raw_demand_usd"], 2.0)
        self.assertEqual(decision.details["selected_buy_effective_demand_usd"], 5.0)
        self.assertEqual(decision.details["selected_buy_demand_usd"], 5.0)
        self.assertEqual(decision.details["min_order_rounded_signals"], 5)
        self.assertEqual(decision.details["min_order_extra_demand_usd"], 3.0)

    def test_min_order_blocked_history_does_not_create_fake_demand(self) -> None:
        config = {
            "risk": {
                "min_order_size_usd": 1.0,
                "max_per_trade_usd": 100.0,
            },
            "sizing": {
                "max_leader_trade_budget_fraction": 1.0,
                "round_up_to_min_order": True,
                "max_min_order_round_up_multiple": 3.0,
            },
            "adaptive_sizing": {
                "enabled": True,
                "lookback_hours": 24.0,
                "target_budget_turnover": 0.20,
                "min_buy_signals_for_history": 5,
            },
        }

        decision = compute_adaptive_sizing_decision(
            leader_wallet="wallet1",
            leader_budget_usd=20.0,
            config=config,
            open_positions=[],
            observations=[self._observation(f"sig-{idx}", notional=1.0) for idx in range(5)],
            processed_signals=[],
            trade_history=[],
            now=datetime(2026, 4, 29, 12, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(decision.multiplier, 1.0)
        self.assertEqual(decision.details["selected_buy_raw_demand_usd"], 1.0)
        self.assertEqual(decision.details["selected_buy_effective_demand_usd"], 0.0)
        self.assertEqual(decision.details["usable_demand_signals"], 0)
        self.assertEqual(decision.details["min_order_blocked_signals"], 5)

    def test_short_batch_metrics_rescue_nearby_small_buys(self) -> None:
        config = {
            "risk": {
                "min_order_size_usd": 1.0,
                "max_per_trade_usd": 100.0,
            },
            "sizing": {
                "max_leader_trade_budget_fraction": 1.0,
                "round_up_to_min_order": True,
                "max_min_order_round_up_multiple": 3.0,
            },
            "signal_batch_coalescer": {
                "enabled": True,
                "window_sec": 30.0,
            },
            "adaptive_sizing": {
                "enabled": True,
                "lookback_hours": 24.0,
                "target_budget_turnover": 0.20,
                "min_buy_signals_for_history": 1,
            },
        }
        rows = [
            self._observation("sig-a", notional=2.0, token_id="tokenA", observed_at="2026-04-29 10:00:00"),
            self._observation("sig-b", notional=2.0, token_id="tokenA", observed_at="2026-04-29 10:00:12"),
            self._observation("sig-c", notional=2.0, token_id="tokenA", observed_at="2026-04-29 10:00:24"),
        ]

        decision = compute_adaptive_sizing_decision(
            leader_wallet="wallet1",
            leader_budget_usd=20.0,
            config=config,
            open_positions=[],
            observations=rows,
            processed_signals=[],
            trade_history=[],
            now=datetime(2026, 4, 29, 12, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(decision.details["signal_batch_coalescer_enabled"], True)
        self.assertEqual(decision.details["buy_signals_7d"], 3)
        self.assertEqual(decision.details["raw_executable_buy_signals_7d"], 0)
        self.assertEqual(decision.details["batch_executable_buy_signals_7d"], 3)
        self.assertEqual(decision.details["batch_executable_buy_orders_7d"], 1)
        self.assertEqual(decision.details["dust_buy_signals_7d"], 0)
        self.assertAlmostEqual(decision.details["median_trade_fraction_7d"], 0.02)

    def test_historical_pressure_ignores_buy_orderbook_share_minimum(self) -> None:
        config = {
            "risk": {
                "min_order_size_usd": 1.0,
                "max_per_trade_usd": 100.0,
            },
            "sizing": {
                "max_leader_trade_budget_fraction": 1.0,
                "round_up_to_min_order": True,
                "max_min_order_round_up_multiple": 3.0,
            },
            "adaptive_sizing": {
                "enabled": True,
                "lookback_hours": 24.0,
                "target_budget_turnover": 0.20,
                "min_buy_signals_for_history": 5,
            },
        }
        rows = [self._observation(f"sig-{idx}", notional=2.0) for idx in range(5)]
        for row in rows:
            row["snapshot_min_order_usd"] = 2.85

        decision = compute_adaptive_sizing_decision(
            leader_wallet="wallet1",
            leader_budget_usd=20.0,
            config=config,
            open_positions=[],
            observations=rows,
            processed_signals=[],
            trade_history=[],
            now=datetime(2026, 4, 29, 12, 0, tzinfo=timezone.utc),
        )

        self.assertAlmostEqual(decision.multiplier, 0.8)
        self.assertEqual(decision.details["selected_buy_raw_demand_usd"], 2.0)
        self.assertEqual(decision.details["selected_buy_effective_demand_usd"], 5.0)
        self.assertEqual(decision.details["min_order_rounded_signals"], 5)

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
