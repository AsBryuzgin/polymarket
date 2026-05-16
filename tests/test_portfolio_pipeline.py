from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from app.final_portfolio_candidates_demo import (
    deduplicate_wallets,
    save_csv as save_candidates_csv,
    select_by_category,
)
from app.portfolio_allocation_demo import load_csv as load_allocation_csv


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
                        "leaderboard_week_pnl": 12.0,
                        "leaderboard_month_pnl": 123.0,
                        "profile_week_pnl": 8.0,
                        "profile_month_pnl": 22.0,
                        "leaderboard_volume": 456.0,
                        "rank": 1,
                        "time_period": "MONTH",
                        "eligible": True,
                        "filter_reasons": "",
                        "median_spread": 0.01,
                        "median_liquidity": 10000.0,
                        "slippage_proxy": 0.005,
                        "current_position_pnl_ratio": 0.10,
                        "total_pnl_ratio": 0.08,
                        "open_loss_exposure": 0.20,
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
        self.assertEqual(rows[0]["current_position_pnl_ratio"], "0.1")
        self.assertEqual(rows[0]["total_pnl_ratio"], "0.08")
        self.assertEqual(rows[0]["open_loss_exposure"], "0.2")
        self.assertEqual(rows[0]["profile_week_pnl"], "8.0")
        self.assertEqual(rows[0]["profile_month_pnl"], "22.0")

    def test_allocation_loader_defensively_filters_ineligible_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "final_portfolio_candidates.csv"
            with path.open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=[
                        "wallet",
                        "user_name",
                        "category",
                        "final_wss",
                        "leaderboard_pnl",
                        "leaderboard_volume",
                        "eligible",
                    ],
                )
                writer.writeheader()
                writer.writerows(
                    [
                        {
                            "wallet": "eligible-wallet",
                            "user_name": "Good",
                            "category": "FINANCE",
                            "final_wss": "70",
                            "leaderboard_pnl": "100",
                            "leaderboard_volume": "1000",
                            "eligible": "True",
                        },
                        {
                            "wallet": "ineligible-wallet",
                            "user_name": "Bad",
                            "category": "FINANCE",
                            "final_wss": "95",
                            "leaderboard_pnl": "1000",
                            "leaderboard_volume": "10000",
                            "eligible": "False",
                        },
                    ]
                )

            rows = load_allocation_csv(path)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["wallet"], "eligible-wallet")

    def test_deduplicate_wallets_prefers_better_rank_when_wss_ties(self) -> None:
        rows = deduplicate_wallets(
            [
                {
                    "wallet": "wallet1",
                    "category": "POLITICS",
                    "all_categories": "POLITICS",
                    "final_wss": 70.0,
                    "rank": 30,
                },
                {
                    "wallet": "wallet1",
                    "category": "FINANCE",
                    "all_categories": "FINANCE",
                    "final_wss": 70.0,
                    "rank": 17,
                },
            ]
        )

        self.assertEqual(rows[0]["category"], "FINANCE")
        self.assertEqual(rows[0]["all_categories"], "FINANCE, POLITICS")

    def test_final_candidates_filters_runtime_economic_copyability_failures(self) -> None:
        rows = select_by_category(
            [
                {
                    "wallet": "dust",
                    "category": "SPORTS",
                    "eligible": True,
                    "final_wss": 80.0,
                    "economic_copyability_status": "FAIL",
                },
                {
                    "wallet": "ok",
                    "category": "SPORTS",
                    "eligible": True,
                    "final_wss": 60.0,
                    "economic_copyability_status": "PASS",
                },
            ],
            quota_per_category=2,
        )

        self.assertEqual([row["wallet"] for row in rows], ["ok"])


if __name__ == "__main__":
    unittest.main()
