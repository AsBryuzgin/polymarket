from __future__ import annotations

import csv
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

from app import rebalance_review


class RebalanceReviewTests(unittest.TestCase):
    def test_write_review_xlsx_contains_formula_sheet(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "review.xlsx"
            rebalance_review.write_review_xlsx(
                [
                    {
                        "category": "FINANCE",
                        "user_name": "cry.eth2",
                        "final_wss": 65.5,
                        "raw_wss": 74.8,
                        "consistency_score": 80.0,
                        "drawdown_score": 70.0,
                        "specialization_score": 60.0,
                        "copyability_score": 90.0,
                        "return_quality_score": 50.0,
                        "track_record_multiplier": 1.0,
                        "data_depth_multiplier": 0.9,
                    }
                ],
                path,
            )

            with zipfile.ZipFile(path) as zf:
                names = set(zf.namelist())
                sheet_xml = zf.read("xl/worksheets/sheet1.xml").decode("utf-8")
                workbook_xml = zf.read("xl/workbook.xml").decode("utf-8")

        self.assertIn("xl/worksheets/sheet1.xml", names)
        self.assertIn("xl/worksheets/sheet2.xml", names)
        self.assertIn("<f>0.35*", sheet_xml)
        self.assertIn('fullCalcOnLoad="1"', workbook_xml)

    def test_validate_review_rows_rejects_stale_shortlist_without_components(self) -> None:
        rows = [
            {
                "category": "FINANCE",
                "user_name": "OldExport",
                "final_wss": "65",
                "raw_wss": "75",
            }
        ]

        with self.assertRaisesRegex(RuntimeError, "missing scoring columns"):
            rebalance_review._validate_review_rows(rows)

    def test_manual_pick_replaces_category_and_reweights(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            all_csv = root / "all.csv"
            live_csv = root / "live.csv"
            report_csv = root / "report.csv"
            pending_json = root / "pending.json"

            fieldnames = [
                "category",
                "rank",
                "user_name",
                "wallet",
                "eligible",
                "final_wss",
                "days_since_last_trade",
                "current_position_pnl_ratio",
            ]
            with all_csv.open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(
                    [
                        {
                            "category": "FINANCE",
                            "rank": "1",
                            "user_name": "LeaderA",
                            "wallet": "wallet-a",
                            "eligible": "True",
                            "final_wss": "70",
                        },
                        {
                            "category": "FINANCE",
                            "rank": "2",
                            "user_name": "LeaderB",
                            "wallet": "wallet-b",
                            "eligible": "True",
                            "final_wss": "60",
                        },
                    ]
                )

            with live_csv.open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=["category", "user_name", "wallet", "final_wss", "weight", "raw_weight"],
                )
                writer.writeheader()
                writer.writerows(
                    [
                        {
                            "category": "FINANCE",
                            "user_name": "Old",
                            "wallet": "old",
                            "final_wss": "50",
                            "weight": "0.5",
                            "raw_weight": "0.5",
                        },
                        {
                            "category": "CULTURE",
                            "user_name": "Culture",
                            "wallet": "culture",
                            "final_wss": "50",
                            "weight": "0.5",
                            "raw_weight": "0.5",
                        },
                    ]
                )

            review = {
                "review_id": "test",
                "status": "PENDING",
                "manual_overrides": {},
                "files": {"all_csv": str(all_csv), "live": str(live_csv), "report": str(report_csv)},
                "proposed_live": [],
            }
            pending_json.write_text(json.dumps(review), encoding="utf-8")

            with patch.object(rebalance_review, "PENDING_FILE", pending_json):
                result = rebalance_review.apply_manual_pick("FINANCE", 2)

            with live_csv.open("r", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            report_exists = report_csv.exists()

        self.assertEqual(result["chosen"]["user_name"], "LeaderB")
        self.assertEqual(rows[0]["user_name"], "LeaderB")
        self.assertAlmostEqual(sum(float(row["weight"]) for row in rows), 1.0, places=6)
        self.assertTrue(report_exists)


if __name__ == "__main__":
    unittest.main()
