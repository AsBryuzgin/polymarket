from __future__ import annotations

import unittest

from execution.order_policy import evaluate_order_policy


class OrderPolicyTests(unittest.TestCase):
    def test_dynamic_spread_allows_relative_tolerance(self) -> None:
        decision = evaluate_order_policy(
            side="BUY",
            midpoint=0.50,
            spread=0.035,
            leader_budget_usd=10.0,
            buy_min_price=0.05,
            buy_max_price=0.95,
            sell_min_price=0.0,
            sell_max_price=1.0,
            max_spread=0.03,
            min_order_size_usd=1.0,
            max_spread_rel=0.08,
            max_spread_hard=0.06,
        )

        self.assertTrue(decision.allowed)

    def test_dynamic_spread_respects_hard_cap(self) -> None:
        decision = evaluate_order_policy(
            side="BUY",
            midpoint=0.90,
            spread=0.07,
            leader_budget_usd=10.0,
            buy_min_price=0.05,
            buy_max_price=0.95,
            sell_min_price=0.0,
            sell_max_price=1.0,
            max_spread=0.03,
            min_order_size_usd=1.0,
            max_spread_rel=0.10,
            max_spread_hard=0.06,
        )

        self.assertFalse(decision.allowed)
        self.assertIn("max_allowed_spread 0.0600", decision.reason)


if __name__ == "__main__":
    unittest.main()
