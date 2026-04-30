from __future__ import annotations

import unittest

from execution.price_drift import price_drift_ok


class PriceDriftTests(unittest.TestCase):
    def test_buy_allows_small_absolute_drift_even_when_relative_is_high(self) -> None:
        allowed, reason = price_drift_ok(
            leader_price=0.20,
            current_price=0.21,
            side="BUY",
            max_abs=0.02,
            max_rel=0.03,
        )

        self.assertTrue(allowed)
        self.assertEqual(reason, "ok")

    def test_buy_blocks_only_when_both_thresholds_are_exceeded(self) -> None:
        allowed, reason = price_drift_ok(
            leader_price=0.50,
            current_price=0.54,
            side="BUY",
            max_abs=0.02,
            max_rel=0.03,
        )

        self.assertFalse(allowed)
        self.assertIn("buy price drift", reason)

    def test_sell_allows_small_absolute_drift_even_when_relative_is_high(self) -> None:
        allowed, reason = price_drift_ok(
            leader_price=0.20,
            current_price=0.19,
            side="SELL",
            max_abs=0.02,
            max_rel=0.03,
        )

        self.assertTrue(allowed)
        self.assertEqual(reason, "ok")


if __name__ == "__main__":
    unittest.main()
