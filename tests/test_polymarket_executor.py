from __future__ import annotations

import unittest
from unittest.mock import patch

from execution.polymarket_executor import (
    _extract_best_bid_ask,
    fetch_market_snapshot,
    preview_market_order,
    preview_market_order_shares,
)


class PolymarketExecutorTests(unittest.TestCase):
    def test_extract_best_bid_ask_accepts_dict_order_books(self) -> None:
        book = {
            "bids": [{"price": "0.38"}, {"price": "0.41"}],
            "asks": [{"price": "0.45"}, {"price": "0.44"}],
        }

        best_bid, best_ask = _extract_best_bid_ask(book)

        self.assertEqual(best_bid, 0.41)
        self.assertEqual(best_ask, 0.44)

    def test_extract_best_bid_ask_accepts_v2_aliases_and_tuple_levels(self) -> None:
        book = {
            "buys": [("0.37", "10"), ("0.39", "5")],
            "sells": [("0.43", "1"), ("0.42", "2")],
        }

        best_bid, best_ask = _extract_best_bid_ask(book)

        self.assertEqual(best_bid, 0.39)
        self.assertEqual(best_ask, 0.42)

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

    def test_preview_sell_shares_uses_share_amount_directly(self) -> None:
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
            result = preview_market_order_shares(token_id="tokenA", share_amount=8.0, side="SELL")

        self.assertEqual(captured["amount"], 8.0)
        self.assertEqual(result["amount_usd"], 4.0)
        self.assertEqual(result["order_amount"], 8.0)
        self.assertEqual(result["order_amount_units"], "shares")

    def test_cached_snapshot_is_hydrated_with_order_book_metadata(self) -> None:
        class Level:
            def __init__(self, price):
                self.price = price

        class Book:
            bids = [Level("0.41")]
            asks = [Level("0.45")]
            min_order_size = "5"
            tick_size = "0.01"
            neg_risk = False

        class FakeClient:
            def get_order_book(self, token_id):
                self.token_id = token_id
                return Book()

        cached = {
            "token_id": "tokenA",
            "side": "BUY",
            "midpoint": 0.43,
            "price_quote": 0.45,
            "best_bid": 0.41,
            "best_ask": 0.45,
            "spread": 0.04,
            "min_order_size": None,
            "tick_size": None,
            "neg_risk": None,
            "source": "market_ws_cache",
        }
        config = {
            "market_cache": {
                "enabled": True,
                "max_age_sec": 10.0,
                "require_orderbook_metadata": True,
            }
        }

        with patch("execution.polymarket_executor.load_executor_config", return_value=config), \
             patch("execution.polymarket_executor.get_market_cache_snapshot", return_value=cached), \
             patch("execution.polymarket_executor.build_authenticated_client", return_value=FakeClient()):
            snapshot = fetch_market_snapshot(token_id="tokenA", side="BUY")

        self.assertEqual(snapshot["source"], "market_ws_cache")
        self.assertEqual(snapshot["price_quote"], 0.45)
        self.assertEqual(snapshot["min_order_size"], 5.0)
        self.assertEqual(snapshot["tick_size"], 0.01)
        self.assertEqual(snapshot["raw_order_book_metadata_source"], "clob_rest")

    def test_sell_snapshot_uses_sell_quote_as_bid_fallback(self) -> None:
        class Book:
            bids = []
            asks = []
            min_order_size = "5"
            tick_size = "0.01"
            neg_risk = False

        class FakeClient:
            def get_midpoint(self, token_id):
                return {"mid": "0.50"}

            def get_price(self, token_id, side):
                return {"price": "0.47"}

            def get_order_book(self, token_id):
                return Book()

        config = {"market_cache": {"enabled": False}}

        with patch("execution.polymarket_executor.load_executor_config", return_value=config), \
             patch("execution.polymarket_executor.build_authenticated_client", return_value=FakeClient()):
            snapshot = fetch_market_snapshot(token_id="tokenA", side="SELL")

        self.assertEqual(snapshot["price_quote"], 0.47)
        self.assertEqual(snapshot["best_bid"], 0.47)
        self.assertIsNone(snapshot["best_ask"])


if __name__ == "__main__":
    unittest.main()
