from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import execution.state_store as state_store
from execution.signal_observation_store import init_signal_observation_table, log_signal_observation
from signals.economic_copyability import (
    annotate_rows_with_economic_copyability,
    compute_budget_volume_coverage_by_wallet,
    compute_economic_copyability_by_wallet,
)


class EconomicCopyabilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self._original_db_path = state_store.DB_PATH
        self.tmpdir = tempfile.TemporaryDirectory()
        state_store.DB_PATH = Path(self.tmpdir.name) / "executor_state.db"
        state_store.init_db()
        init_signal_observation_table()

    def tearDown(self) -> None:
        state_store.DB_PATH = self._original_db_path
        self.tmpdir.cleanup()

    def _log_buy(
        self,
        *,
        wallet: str,
        signal_id: str,
        token_id: str,
        target_budget_usd: float,
        trade_notional_usd: float,
        portfolio_value_usd: float,
    ) -> None:
        log_signal_observation(
            leader_wallet=wallet,
            leader_user_name="Leader",
            category="SPORTS",
            leader_status="ACTIVE",
            target_budget_usd=target_budget_usd,
            latest_trade_side="BUY",
            latest_trade_age_sec=1.0,
            latest_trade_hash=signal_id,
            latest_status="FRESH_COPYABLE",
            latest_reason="copyable",
            selected_signal_id=signal_id,
            selected_side="BUY",
            token_id=token_id,
            selected_trade_age_sec=1.0,
            selected_trade_notional_usd=trade_notional_usd,
            selected_leader_portfolio_value_usd=portfolio_value_usd,
            snapshot_min_order_usd=1.0,
        )

    def test_runtime_economic_copyability_marks_dust_leader_as_fail(self) -> None:
        for idx in range(20):
            self._log_buy(
                wallet="dust-wallet",
                signal_id=f"dust-{idx}",
                token_id="token-dust",
                target_budget_usd=20.0,
                trade_notional_usd=1.0,
                portfolio_value_usd=10000.0,
            )
            self._log_buy(
                wallet="copyable-wallet",
                signal_id=f"copyable-{idx}",
                token_id=f"token-{idx}",
                target_budget_usd=20.0,
                trade_notional_usd=10.0,
                portfolio_value_usd=100.0,
            )

        config = {
            "risk": {"min_order_size_usd": 1.0},
            "sizing": {"max_min_order_round_up_multiple": 3.0},
            "signal_batch_coalescer": {"window_sec": 30.0},
            "economic_copyability": {
                "enabled": True,
                "lookback_hours": 168.0,
                "min_buy_signals": 20,
                "min_executable_ratio": 0.10,
                "min_batchable_ratio": 0.35,
            },
        }
        metrics = compute_economic_copyability_by_wallet(config=config)

        self.assertEqual(metrics["dust-wallet"].status, "FAIL")
        self.assertAlmostEqual(metrics["dust-wallet"].median_trade_fraction, 0.0001)
        self.assertAlmostEqual(metrics["dust-wallet"].mean_trade_fraction, 0.0001)
        self.assertEqual(metrics["copyable-wallet"].status, "PASS")
        self.assertAlmostEqual(metrics["copyable-wallet"].median_trade_fraction, 0.1)
        self.assertAlmostEqual(metrics["copyable-wallet"].mean_trade_fraction, 0.1)
        self.assertAlmostEqual(
            metrics["copyable-wallet"].required_bankroll_p95_volume_usd,
            10.0,
        )

        rows = [
            {"wallet": "dust-wallet", "eligible": True, "filter_reasons": ""},
            {"wallet": "copyable-wallet", "eligible": True, "filter_reasons": ""},
        ]
        annotate_rows_with_economic_copyability(rows, config=config)

        self.assertFalse(rows[0]["eligible"])
        self.assertIn("economic_copyability", rows[0]["filter_reasons"])
        self.assertTrue(rows[1]["eligible"])

    def test_runtime_economic_copyability_can_fail_low_median_trade_fraction(self) -> None:
        for idx in range(20):
            if idx < 3:
                trade_notional = 6.0
                portfolio_value = 100.0
            else:
                trade_notional = 0.05
                portfolio_value = 100.0
            self._log_buy(
                wallet="spiky-wallet",
                signal_id=f"spiky-{idx}",
                token_id=f"token-{idx}",
                target_budget_usd=20.0,
                trade_notional_usd=trade_notional,
                portfolio_value_usd=portfolio_value,
            )

        config = {
            "risk": {"min_order_size_usd": 1.0},
            "sizing": {"max_min_order_round_up_multiple": 1.0},
            "signal_batch_coalescer": {"window_sec": 30.0},
            "economic_copyability": {
                "enabled": True,
                "lookback_hours": 168.0,
                "min_buy_signals": 20,
                "min_executable_ratio": 0.10,
                "min_batchable_ratio": 0.35,
                "min_median_trade_fraction": 0.001,
            },
        }

        metrics = compute_economic_copyability_by_wallet(config=config)

        self.assertEqual(metrics["spiky-wallet"].status, "FAIL")
        self.assertIn("trade fraction", metrics["spiky-wallet"].reason)
        self.assertAlmostEqual(metrics["spiky-wallet"].median_trade_fraction, 0.0005)
        self.assertAlmostEqual(metrics["spiky-wallet"].executable_ratio, 0.15)

    def test_budget_volume_coverage_uses_trade_fraction_volume(self) -> None:
        for idx in range(10):
            self._log_buy(
                wallet="mixed-wallet",
                signal_id=f"large-{idx}",
                token_id=f"large-token-{idx}",
                target_budget_usd=20.0,
                trade_notional_usd=10.0,
                portfolio_value_usd=100.0,
            )
            self._log_buy(
                wallet="mixed-wallet",
                signal_id=f"small-{idx}",
                token_id=f"small-token-{idx}",
                target_budget_usd=20.0,
                trade_notional_usd=1.0,
                portfolio_value_usd=100.0,
            )

        config = {
            "risk": {"min_order_size_usd": 1.0},
            "sizing": {"max_min_order_round_up_multiple": 3.0},
            "signal_batch_coalescer": {"window_sec": 30.0},
            "economic_copyability": {
                "enabled": True,
                "lookback_hours": 168.0,
                "min_buy_signals": 1,
                "min_executable_ratio": 0.0,
                "min_batchable_ratio": 0.0,
            },
        }

        metrics = compute_economic_copyability_by_wallet(config=config)["mixed-wallet"]
        coverage = compute_budget_volume_coverage_by_wallet(
            config=config,
            budget_by_wallet={"mixed-wallet": 10.0},
        )["mixed-wallet"]

        self.assertAlmostEqual(metrics.required_bankroll_p95_volume_usd, 100.0)
        self.assertAlmostEqual(coverage["volume_coverage"], 10 / 11, places=6)
        self.assertAlmostEqual(
            coverage["volume_coverage_with_roundup"],
            10 / 11,
            places=6,
        )


if __name__ == "__main__":
    unittest.main()
