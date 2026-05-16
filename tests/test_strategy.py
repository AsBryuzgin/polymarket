from __future__ import annotations

import unittest

from app.allocation_runtime import resolve_leader_budget_usd, resolve_total_capital_usd
from app.apply_rebalance_lifecycle import load_grace_days, load_total_capital_usd


class RebalanceLifecycleConfigTests(unittest.TestCase):
    def test_lifecycle_capital_comes_from_config(self) -> None:
        cfg = {
            "lifecycle": {"exit_grace_days": 21},
            "capital": {"total_capital_usd": 250.0},
        }

        self.assertEqual(load_grace_days(cfg), 21)
        self.assertEqual(load_total_capital_usd(cfg), 250.0)

    def test_lifecycle_config_defaults_match_pilot_setup(self) -> None:
        self.assertEqual(load_grace_days({}), 14)
        self.assertEqual(load_total_capital_usd({}), 0.0)

    def test_runtime_capital_prefers_executor_config(self) -> None:
        total = resolve_total_capital_usd(
            executor_config={"capital": {"total_capital_usd": 250.0}},
            rebalance_config={"capital": {"total_capital_usd": 50.0}},
        )

        self.assertEqual(total, 250.0)

    def test_runtime_capital_falls_back_to_rebalance_config(self) -> None:
        total = resolve_total_capital_usd(
            executor_config={},
            rebalance_config={"capital": {"total_capital_usd": 150.0}},
        )

        self.assertEqual(total, 150.0)

    def test_leader_budget_prefers_explicit_target_budget(self) -> None:
        budget = resolve_leader_budget_usd(
            {"target_budget_usd": "12.34", "weight": "0.50"},
            total_capital_usd=100.0,
        )

        self.assertEqual(budget, 12.34)

    def test_leader_budget_uses_weight_when_no_explicit_budget(self) -> None:
        budget = resolve_leader_budget_usd(
            {"weight": "0.25"},
            total_capital_usd=200.0,
        )

        self.assertEqual(budget, 50.0)


if __name__ == "__main__":
    unittest.main()
