from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import execution.state_store as state_store
from execution.market_cache import (
    get_market_cache_snapshot,
    init_market_cache_table,
    upsert_market_cache_from_ws,
)


class MarketCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self._original_db_path = state_store.DB_PATH

    def tearDown(self) -> None:
        state_store.DB_PATH = self._original_db_path

    def test_book_event_builds_snapshot_quotes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_store.DB_PATH = Path(tmp) / "executor_state.db"
            state_store.init_db()
            init_market_cache_table()

            updated = upsert_market_cache_from_ws(
                {
                    "event_type": "book",
                    "asset_id": "tokenA",
                    "market": "conditionA",
                    "bids": [{"price": "0.41"}, {"price": "0.39"}],
                    "asks": [{"price": "0.45"}, {"price": "0.47"}],
                }
            )

            self.assertTrue(updated)
            buy_snapshot = get_market_cache_snapshot("tokenA", side="BUY", max_age_sec=60)
            sell_snapshot = get_market_cache_snapshot("tokenA", side="SELL", max_age_sec=60)

            self.assertIsNotNone(buy_snapshot)
            self.assertEqual(buy_snapshot["source"], "market_ws_cache")
            self.assertEqual(buy_snapshot["best_bid"], 0.41)
            self.assertEqual(buy_snapshot["best_ask"], 0.45)
            self.assertEqual(buy_snapshot["price_quote"], 0.45)
            self.assertEqual(sell_snapshot["price_quote"], 0.41)


if __name__ == "__main__":
    unittest.main()

