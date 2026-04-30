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
                "scoring_version": rebalance_review.SCORING_VERSION,
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

    def test_manual_replacement_can_choose_candidate_from_different_category(self) -> None:
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
                "copyability_score",
            ]
            with all_csv.open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(
                    [
                        {
                            "category": "WEATHER",
                            "rank": "1",
                            "user_name": "WeatherLeader",
                            "wallet": "weather-wallet",
                            "eligible": "True",
                            "final_wss": "72",
                            "copyability_score": "80",
                        },
                        {
                            "category": "FINANCE",
                            "rank": "1",
                            "user_name": "FinanceCandidate",
                            "wallet": "finance-candidate",
                            "eligible": "True",
                            "final_wss": "65",
                            "copyability_score": "75",
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
                            "category": "SPORTS",
                            "user_name": "OldSports",
                            "wallet": "sports-wallet",
                            "final_wss": "70",
                            "weight": "0.5",
                            "raw_weight": "0.5",
                        },
                        {
                            "category": "FINANCE",
                            "user_name": "OldFinance",
                            "wallet": "finance-wallet",
                            "final_wss": "60",
                            "weight": "0.5",
                            "raw_weight": "0.5",
                        },
                    ]
                )

            review = {
                "review_id": "test",
                "scoring_version": rebalance_review.SCORING_VERSION,
                "status": "PENDING",
                "manual_overrides": {},
                "files": {"all_csv": str(all_csv), "live": str(live_csv), "report": str(report_csv)},
                "proposed_live": [],
            }
            pending_json.write_text(json.dumps(review), encoding="utf-8")

            with patch.object(rebalance_review, "PENDING_FILE", pending_json):
                result = rebalance_review.apply_manual_replacement(
                    replace_index=1,
                    candidate_category="WEATHER",
                    pick_index=1,
                    review_id="test",
                )

            with live_csv.open("r", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))

        self.assertEqual(result["replaced"]["user_name"], "OldSports")
        self.assertEqual(result["chosen"]["user_name"], "WeatherLeader")
        self.assertEqual(rows[0]["user_name"], "WeatherLeader")
        self.assertEqual(rows[0]["category"], "WEATHER")
        self.assertEqual(rows[1]["user_name"], "OldFinance")
        self.assertAlmostEqual(sum(float(row["weight"]) for row in rows), 1.0, places=6)

    def test_manual_candidate_list_shows_ineligible_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            all_csv = root / "all.csv"
            pending_json = root / "pending.json"

            fieldnames = [
                "category",
                "rank",
                "user_name",
                "wallet",
                "eligible",
                "filter_reasons",
                "final_wss",
                "copyability_score",
            ]
            with all_csv.open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerow(
                    {
                        "category": "SPORTS",
                        "rank": "1",
                        "user_name": "ManualOnly",
                        "wallet": "manual-wallet",
                        "eligible": "False",
                        "filter_reasons": "profile_week_pnl <= 0",
                        "final_wss": "68",
                        "copyability_score": "72",
                    }
                )
            pending_json.write_text(
                json.dumps(
                    {
                        "review_id": "test",
                        "scoring_version": rebalance_review.SCORING_VERSION,
                        "status": "PENDING",
                        "manual_overrides": {},
                        "files": {"all_csv": str(all_csv)},
                        "proposed_live": [],
                    }
                ),
                encoding="utf-8",
            )

            with patch.object(rebalance_review, "PENDING_FILE", pending_json):
                rows = rebalance_review.manual_candidates_for_category("SPORTS")
                categories = rebalance_review.manual_candidate_categories()
                text = rebalance_review.list_manual_candidates("SPORTS")

        self.assertEqual(rows[0]["user_name"], "ManualOnly")
        self.assertIn("SPORTS", categories)
        self.assertIn("eligible=false", text)
        self.assertIn("profile_week_pnl <= 0", text)

    def test_manual_replacement_can_choose_ineligible_candidate(self) -> None:
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
                "filter_reasons",
                "final_wss",
                "copyability_score",
            ]
            with all_csv.open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerow(
                    {
                        "category": "SPORTS",
                        "rank": "1",
                        "user_name": "IneligibleSports",
                        "wallet": "sports-new",
                        "eligible": "False",
                        "filter_reasons": "copyability_score < 60",
                        "final_wss": "64",
                        "copyability_score": "55",
                    }
                )

            with live_csv.open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=["category", "user_name", "wallet", "eligible", "final_wss", "weight", "raw_weight"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "category": "POLITICS",
                        "user_name": "OldPolitics",
                        "wallet": "politics-wallet",
                        "eligible": "True",
                        "final_wss": "70",
                        "weight": "1.0",
                        "raw_weight": "1.0",
                    }
                )

            review = {
                "review_id": "test",
                "scoring_version": rebalance_review.SCORING_VERSION,
                "status": "PENDING",
                "manual_overrides": {},
                "files": {"all_csv": str(all_csv), "live": str(live_csv), "report": str(report_csv)},
                "proposed_live": [],
            }
            pending_json.write_text(json.dumps(review), encoding="utf-8")

            with patch.object(rebalance_review, "PENDING_FILE", pending_json):
                result = rebalance_review.apply_manual_replacement(
                    replace_index=1,
                    candidate_category="SPORTS",
                    pick_index=1,
                    review_id="test",
                )

            with live_csv.open("r", encoding="utf-8") as f:
                live_rows = list(csv.DictReader(f))
            with report_csv.open("r", encoding="utf-8") as f:
                report_rows = list(csv.DictReader(f))

        self.assertEqual(result["chosen"]["user_name"], "IneligibleSports")
        self.assertEqual(live_rows[0]["user_name"], "IneligibleSports")
        self.assertEqual(live_rows[0]["eligible"], "False")
        self.assertIn("manual ineligible override", report_rows[0]["reason"])
        self.assertIn("copyability_score < 60", report_rows[0]["reason"])

    def test_approve_rejects_stale_pending_review_without_components(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            all_csv = root / "all.csv"
            pending_json = root / "pending.json"
            with all_csv.open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=["category", "user_name", "wallet", "eligible", "final_wss", "raw_wss"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "category": "FINANCE",
                        "user_name": "OldExport",
                        "wallet": "wallet",
                        "eligible": "True",
                        "final_wss": "65",
                        "raw_wss": "75",
                    }
                )
            pending_json.write_text(
                json.dumps(
                    {
                        "review_id": "stale",
                        "scoring_version": rebalance_review.SCORING_VERSION,
                        "status": "PENDING",
                        "files": {
                            "all_csv": str(all_csv),
                            "final_candidates": str(root / "final.csv"),
                            "final_allocation": str(root / "alloc.csv"),
                            "live": str(root / "live.csv"),
                            "report": str(root / "report.csv"),
                            "state": str(root / "state.json"),
                        },
                    }
                ),
                encoding="utf-8",
            )

            with patch.object(rebalance_review, "PENDING_FILE", pending_json):
                with self.assertRaisesRegex(RuntimeError, "missing scoring columns"):
                    rebalance_review.approve_pending_review("stale")


if __name__ == "__main__":
    unittest.main()
