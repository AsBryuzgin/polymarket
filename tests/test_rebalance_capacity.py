from __future__ import annotations

import unittest

from app.build_live_universe_stable import apply_live_category_capacity


class RebalanceCapacityTests(unittest.TestCase):
    def test_live_category_capacity_keeps_top_categories_by_selected_wss(self) -> None:
        selected_rows = [
            {"category": "SPORTS", "final_wss": 76.23, "weight": 0.12},
            {"category": "ECONOMICS", "final_wss": 80.51, "weight": 0.13},
            {"category": "TECH", "final_wss": 78.36, "weight": 0.12},
            {"category": "FINANCE", "final_wss": 80.24, "weight": 0.13},
            {"category": "CULTURE", "final_wss": 79.63, "weight": 0.13},
        ]
        report_rows = [
            {"category": row["category"], "selected_wss": row["final_wss"]}
            for row in selected_rows
        ]

        live_rows, report = apply_live_category_capacity(
            selected_rows=selected_rows,
            report_rows=report_rows,
            max_live_categories=4,
        )

        self.assertEqual(
            [row["category"] for row in live_rows],
            ["ECONOMICS", "TECH", "FINANCE", "CULTURE"],
        )
        self.assertEqual(
            {row["category"] for row in live_rows},
            {"ECONOMICS", "FINANCE", "CULTURE", "TECH"},
        )

        report_by_category = {row["category"]: row for row in report}
        self.assertEqual(report_by_category["ECONOMICS"]["live_rank"], 1)
        self.assertEqual(report_by_category["SPORTS"]["live_included"], "NO")
        self.assertIn("excluded", report_by_category["SPORTS"]["live_capacity_reason"])


if __name__ == "__main__":
    unittest.main()
