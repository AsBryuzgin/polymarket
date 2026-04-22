from __future__ import annotations

import unittest
from unittest.mock import patch

from execution.polymarket_executor import preview_market_order


class PolymarketExecutorTests(unittest.TestCase):
    def test_preview_sell_converts_usd_amount_to_shares(self) -> None:
        captured = {}

        class FakeClient:
            def create_market_order(self, order):
                captured["amount"] = order.amount
                return order

        snapshot = {
            "midpoint": 0.50,
            "price_quote": 0.50,
            "best_bid": 0.49,
            "best_ask": 0.51,
            "spread": 0.02,
            "min_order_size": 1.0,
            "tick_size": 0.01,
            "neg_risk": False,
        }

        with patch("execution.polymarket_executor.build_authenticated_client", return_value=FakeClient()), \
             patch("execution.polymarket_executor.fetch_market_snapshot", return_value=snapshot):
            result = preview_market_order(token_id="tokenA", amount_usd=2.0, side="SELL")

        self.assertEqual(captured["amount"], 4.0)
        self.assertEqual(result["order_amount"], 4.0)
        self.assertEqual(result["order_amount_units"], "shares")


if __name__ == "__main__":
    unittest.main()
