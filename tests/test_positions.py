from __future__ import annotations

import unittest

from execution.positions import normalize_exchange_open_order, normalize_exchange_position


class ExchangePositionNormalizationTests(unittest.TestCase):
    def test_normalize_data_api_position_shape(self) -> None:
        row = normalize_exchange_position(
            {
                "asset": "tokenA",
                "size": "4.5",
                "currentValue": "2.25",
                "avgPrice": "0.50",
                "conditionId": "condA",
                "slug": "market-a",
                "outcome": "YES",
            }
        )

        self.assertIsNotNone(row)
        self.assertEqual(row["token_id"], "tokenA")
        self.assertEqual(row["size"], 4.5)
        self.assertEqual(row["current_value_usd"], 2.25)
        self.assertEqual(row["avg_price"], 0.50)

    def test_normalize_open_order_shape(self) -> None:
        row = normalize_exchange_open_order(
            {
                "id": "order1",
                "asset_id": "tokenA",
                "side": "BUY",
                "price": "0.42",
                "original_size": "10",
                "size_matched": "3",
                "status": "OPEN",
            }
        )

        self.assertIsNotNone(row)
        self.assertEqual(row["token_id"], "tokenA")
        self.assertEqual(row["remaining_size"], 7.0)


if __name__ == "__main__":
    unittest.main()
