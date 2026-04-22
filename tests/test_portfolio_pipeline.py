from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from app.final_portfolio_candidates_demo import save_csv as save_candidates_csv


class PortfolioPipelineTests(unittest.TestCase):
    def test_final_candidates_csv_accepts_activity_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "final_portfolio_candidates.csv"
            save_candidates_csv(
                [
                    {
                        "wallet": "wallet1",
                        "user_name": "Leader",
                        "category": "CRYPTO",
                        "all_categories": "CRYPTO",
                        "final_wss": 70.0,
                        "raw_wss": 72.0,
                        "activity_score": 100.0,
                        "leaderboard_pnl": 123.0,
                        "leaderboard_volume": 456.0,
                        "rank": 1,
                        "time_period": "MONTH",
                        "eligible": True,
                        "filter_reasons": "",
                        "median_spread": 0.01,
                        "median_liquidity": 10000.0,
                        "slippage_proxy": 0.005,
                        "trades_30d": 12,
                        "trades_90d": 30,
                        "days_since_last_trade": 1,
                        "closed_positions_used": 100,
                    }
                ],
                path,
            )

            with path.open("r", encoding="utf-8", newline="") as f:
                rows = list(csv.DictReader(f))

        self.assertEqual(rows[0]["activity_score"], "100.0")
        self.assertEqual(rows[0]["trades_30d"], "12")


if __name__ == "__main__":
    unittest.main()
