from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import execution.state_store as state_store
from execution.copy_worker import LeaderSignal, process_signal
from risk.guards import build_runtime_risk_limits, evaluate_buy_guards
from risk.limits import RiskLimits
from risk.sizing import compute_signal_copy_amount


class RiskGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "test_executor_state.db"
        state_store.DB_PATH = self.db_path
        state_store.init_db()

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_leader_budget_cap_blocks_wallet_overexposure(self) -> None:
        decision = evaluate_buy_guards(
            limits=RiskLimits(
                min_order_size_usd=1.0,
                max_per_trade_usd=5.0,
                enforce_leader_budget_cap=True,
            ),
            leader_wallet="wallet1",
            token_id="tokenB",
            amount_usd=1.0,
            leader_budget_usd=2.0,
            category="SPORTS",
            open_positions=[
                {
                    "leader_wallet": "wallet1",
                    "token_id": "tokenA",
                    "position_usd": 1.5,
                    "category": "SPORTS",
                }
            ],
        )

        self.assertFalse(decision.allowed)
        self.assertIn("above leader budget", decision.reason)
        self.assertEqual(decision.details["wallet_exposure_after_usd"], 2.5)

    def test_portfolio_cap_blocks_new_entry(self) -> None:
        decision = evaluate_buy_guards(
            limits=RiskLimits(
                min_order_size_usd=1.0,
                max_per_trade_usd=5.0,
                max_portfolio_exposure_usd=3.0,
                enforce_leader_budget_cap=False,
            ),
            leader_wallet="wallet2",
            token_id="tokenB",
            amount_usd=1.0,
            leader_budget_usd=20.0,
            category="CRYPTO",
            open_positions=[
                {"leader_wallet": "wallet1", "token_id": "tokenA", "position_usd": 2.5},
            ],
        )

        self.assertFalse(decision.allowed)
        self.assertIn("portfolio exposure", decision.reason)

    def test_daily_loss_guard_blocks_new_entry(self) -> None:
        decision = evaluate_buy_guards(
            limits=RiskLimits(
                min_order_size_usd=1.0,
                max_per_trade_usd=5.0,
                max_daily_realized_loss_usd=2.0,
                enforce_leader_budget_cap=False,
            ),
            leader_wallet="wallet2",
            token_id="tokenB",
            amount_usd=1.0,
            leader_budget_usd=20.0,
            category="CRYPTO",
            open_positions=[],
            realized_pnl_today_usd=-2.0,
        )

        self.assertFalse(decision.allowed)
        self.assertIn("daily realized loss", decision.reason)

    def test_percent_limits_resolve_from_capital_base(self) -> None:
        limits = RiskLimits.from_config(
            {
                "risk": {
                    "max_per_trade_pct": 0.05,
                    "max_position_pct": 0.08,
                    "max_portfolio_exposure_pct": 0.90,
                    "max_daily_realized_loss_pct": 0.075,
                }
            },
            capital_base_usd=200.0,
        )

        self.assertEqual(limits.max_per_trade_usd, 10.0)
        self.assertEqual(limits.max_position_usd, 16.0)
        self.assertEqual(limits.max_portfolio_exposure_usd, 180.0)
        self.assertEqual(limits.max_daily_realized_loss_usd, 15.0)

    def test_percent_limits_block_without_capital_base(self) -> None:
        limits = RiskLimits.from_config(
            {"risk": {"max_per_trade_pct": 0.05}},
            capital_base_error="balance fetch failed",
        )

        decision = evaluate_buy_guards(
            limits=limits,
            leader_wallet="wallet2",
            token_id="tokenB",
            amount_usd=1.0,
            leader_budget_usd=20.0,
            category="CRYPTO",
            open_positions=[],
        )

        self.assertFalse(decision.allowed)
        self.assertIn("account collateral balance", decision.reason)

    def test_runtime_percent_limits_use_collateral_balance_source(self) -> None:
        config = {
            "capital": {"source": "collateral_balance"},
            "risk": {
                "max_per_trade_pct": 0.05,
                "max_wallet_exposure_pct": 0.12,
            },
        }

        with patch("execution.allowance.fetch_collateral_balance_allowance") as fetch_balance:
            fetch_balance.return_value.balance_usd = 150.0
            limits = build_runtime_risk_limits(config)

        self.assertEqual(limits.capital_base_usd, 150.0)
        self.assertEqual(limits.max_per_trade_usd, 7.5)
        self.assertEqual(limits.max_wallet_exposure_usd, 18.0)

    def test_copy_worker_records_risk_skip_before_preview(self) -> None:
        state_store.upsert_buy_position(
            leader_wallet="wallet3",
            token_id="tokenA",
            amount_usd=1.5,
            entry_price=0.50,
            signal_id="seed-buy",
        )

        signal = LeaderSignal(
            signal_id="sig-buy-risk-block",
            leader_wallet="wallet3",
            token_id="tokenB",
            side="BUY",
            leader_budget_usd=2.0,
            leader_trade_notional_usd=10.0,
            leader_portfolio_value_usd=10.0,
        )

        config = {
            "risk": {
                "min_order_size_usd": 0.01,
                "max_per_trade_usd": 5.0,
                "max_wallet_exposure_usd": 1.75,
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
             patch("execution.copy_worker.preview_market_order") as preview:
            result = process_signal(signal)

        preview.assert_not_called()
        self.assertEqual(result["status"], "SKIPPED_RISK")
        self.assertIn("max_wallet_exposure", result["reason"])

        pos_a = state_store.get_open_position("wallet3", "tokenA")
        self.assertIsNotNone(pos_a)
        self.assertEqual(float(pos_a["position_usd"]), 1.5)
        self.assertIsNone(state_store.get_open_position("wallet3", "tokenB"))

        self.assertTrue(state_store.has_signal("sig-buy-risk-block"))

    def test_signal_sizing_uses_leader_trade_budget_fraction(self) -> None:
        decision = compute_signal_copy_amount(
            leader_budget_usd=12.0,
            remaining_leader_budget_usd=12.0,
            leader_trade_notional_usd=50.0,
            leader_trade_notional_copy_fraction=0.20,
            leader_portfolio_value_usd=1000.0,
            min_order_size_usd=0.01,
            max_per_trade_usd=10.0,
        )

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.amount_usd, 0.6)
        self.assertEqual(decision.source, "leader_trade_budget_fraction")

    def test_signal_sizing_uses_remaining_leader_budget(self) -> None:
        decision = compute_signal_copy_amount(
            leader_budget_usd=12.0,
            remaining_leader_budget_usd=9.0,
            leader_trade_notional_usd=50.0,
            leader_trade_notional_copy_fraction=0.20,
            leader_portfolio_value_usd=1000.0,
            min_order_size_usd=0.01,
            max_per_trade_usd=10.0,
        )

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.amount_usd, 0.45)
        self.assertEqual(decision.source, "leader_trade_budget_fraction")

    def test_signal_sizing_applies_adaptive_multiplier(self) -> None:
        decision = compute_signal_copy_amount(
            leader_budget_usd=12.0,
            remaining_leader_budget_usd=12.0,
            leader_trade_notional_usd=50.0,
            leader_trade_notional_copy_fraction=0.20,
            leader_portfolio_value_usd=1000.0,
            min_order_size_usd=0.01,
            max_per_trade_usd=10.0,
            adaptive_size_multiplier=0.50,
        )

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.amount_usd, 0.3)
        self.assertTrue(decision.details["adaptive_size_multiplier_applied"])

    def test_signal_sizing_does_not_round_up_to_minimum(self) -> None:
        decision = compute_signal_copy_amount(
            leader_budget_usd=12.0,
            remaining_leader_budget_usd=12.0,
            leader_trade_notional_usd=50.0,
            leader_trade_notional_copy_fraction=0.20,
            leader_portfolio_value_usd=1000.0,
            min_order_size_usd=1.0,
            max_per_trade_usd=10.0,
        )

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.source, "leader_trade_budget_fraction")
        self.assertIn("below min_order_size_usd", decision.reason)

    def test_signal_sizing_can_round_up_to_market_minimum(self) -> None:
        decision = compute_signal_copy_amount(
            leader_budget_usd=12.0,
            remaining_leader_budget_usd=12.0,
            leader_trade_notional_usd=50.0,
            leader_trade_notional_copy_fraction=0.20,
            leader_portfolio_value_usd=1000.0,
            min_order_size_usd=1.85,
            max_per_trade_usd=10.0,
            round_up_to_min_order=True,
        )

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.amount_usd, 1.85)
        self.assertEqual(decision.reason, "rounded up to min_order_size_usd")
        self.assertTrue(decision.details["min_order_round_up_applied"])
        self.assertAlmostEqual(decision.details["pre_min_order_round_amount_usd"], 0.6)

    def test_signal_sizing_does_not_round_up_past_caps(self) -> None:
        decision = compute_signal_copy_amount(
            leader_budget_usd=12.0,
            remaining_leader_budget_usd=12.0,
            leader_trade_notional_usd=50.0,
            leader_trade_notional_copy_fraction=0.20,
            leader_portfolio_value_usd=1000.0,
            min_order_size_usd=1.85,
            max_per_trade_usd=1.0,
            round_up_to_min_order=True,
        )

        self.assertFalse(decision.allowed)
        self.assertIn("max_per_trade_usd below", decision.reason)

    def test_signal_sizing_blocks_without_leader_portfolio_by_default(self) -> None:
        decision = compute_signal_copy_amount(
            leader_budget_usd=12.0,
            leader_trade_notional_usd=50.0,
            leader_trade_notional_copy_fraction=0.20,
            min_order_size_usd=0.01,
            max_per_trade_usd=10.0,
        )

        self.assertFalse(decision.allowed)
        self.assertIn("portfolio value unavailable", decision.reason)

    def test_signal_sizing_legacy_notional_fallback_is_opt_in(self) -> None:
        decision = compute_signal_copy_amount(
            leader_budget_usd=20.0,
            leader_trade_notional_usd=15.0,
            leader_trade_notional_copy_fraction=0.20,
            min_order_size_usd=1.0,
            max_per_trade_usd=10.0,
            allow_notional_fallback=True,
        )

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.amount_usd, 3.0)
        self.assertEqual(decision.source, "leader_trade_notional")

    def test_signal_sizing_budget_fallback_is_opt_in(self) -> None:
        decision = compute_signal_copy_amount(
            leader_budget_usd=3.5,
            leader_trade_notional_usd=None,
            leader_trade_notional_copy_fraction=0.20,
            min_order_size_usd=1.0,
            max_per_trade_usd=10.0,
            allow_budget_fallback=True,
        )

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.amount_usd, 3.5)
        self.assertEqual(decision.source, "fallback_budget")

    def test_signal_sizing_blocks_if_caps_make_min_order_impossible(self) -> None:
        decision = compute_signal_copy_amount(
            leader_budget_usd=20.0,
            leader_trade_notional_usd=100.0,
            leader_trade_notional_copy_fraction=0.20,
            leader_portfolio_value_usd=1000.0,
            min_order_size_usd=2.0,
            max_per_trade_usd=1.0,
        )

        self.assertFalse(decision.allowed)
        self.assertIn("max_per_trade_usd below", decision.reason)


if __name__ == "__main__":
    unittest.main()
