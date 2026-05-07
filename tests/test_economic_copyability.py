from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import execution.state_store as state_store
from execution.signal_observation_store import init_signal_observation_table, log_signal_observation
from signals.economic_copyability import (
    annotate_rows_with_economic_copyability,
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
        self.assertEqual(metrics["copyable-wallet"].status, "PASS")

        rows = [
            {"wallet": "dust-wallet", "eligible": True, "filter_reasons": ""},
            {"wallet": "copyable-wallet", "eligible": True, "filter_reasons": ""},
        ]
        annotate_rows_with_economic_copyability(rows, config=config)

        self.assertFalse(rows[0]["eligible"])
        self.assertIn("economic_copyability", rows[0]["filter_reasons"])
        self.assertTrue(rows[1]["eligible"])


if __name__ == "__main__":
    unittest.main()
