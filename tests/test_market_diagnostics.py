from __future__ import annotations

import unittest
from unittest.mock import patch

from execution.market_diagnostics import (
    _extract_token_meta_from_market,
    diagnose_market_snapshot_error,
)


class MarketDiagnosticsTests(unittest.TestCase):
    def test_extract_token_meta_falls_back_to_outcome_price_arrays(self) -> None:
        meta = _extract_token_meta_from_market(
            {
                "tokens": None,
                "clobTokenIds": '["tokenA","tokenB"]',
                "outcomes": '["Yes","No"]',
                "outcomePrices": '["1","0"]',
            },
            "tokenA",
        )

        self.assertEqual(meta["outcome"], "Yes")
        self.assertTrue(meta["winner"])
        self.assertEqual(meta["price"], 1.0)

    def test_no_orderbook_closed_market_is_classified_as_closed(self) -> None:
        with patch(
            "execution.market_diagnostics.lookup_token_market",
            return_value={
                "question": "Will it rain?",
                "slug": "will-it-rain",
                "condition_id": "cond-1",
                "active": False,
                "closed": True,
                "archived": False,
                "accepting_orders": False,
                "enable_order_book": False,
                "uma_resolution_status": "resolved",
            },
        ):
            diagnosis = diagnose_market_snapshot_error(
                "token-1",
                "404 No orderbook exists for the requested token id",
            )

        self.assertEqual(diagnosis["diagnosis_status"], "NO_ORDERBOOK_CLOSED_OR_RESOLVED")
        self.assertEqual(diagnosis["question"], "Will it rain?")

    def test_no_orderbook_active_market_is_marked_suspicious(self) -> None:
        with patch(
            "execution.market_diagnostics.lookup_token_market",
            return_value={
                "question": "Will BTC close above 100k?",
                "slug": "btc-above-100k",
                "condition_id": "cond-2",
                "active": True,
                "closed": False,
                "archived": False,
                "accepting_orders": True,
                "enable_order_book": True,
                "uma_resolution_status": "",
            },
        ):
            diagnosis = diagnose_market_snapshot_error(
                "token-2",
                "No orderbook exists for the requested token id",
            )

        self.assertEqual(diagnosis["diagnosis_status"], "NO_ORDERBOOK_ACTIVE_MARKET")


if __name__ == "__main__":
    unittest.main()
