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

    def test_live_summary_includes_budget_volume_coverage(self) -> None:
        text = rebalance_review._summarize_live_rows(
            [
                {
                    "user_name": "Copyable",
                    "category": "SPORTS",
                    "final_wss": "70",
                    "weight": "0.5",
                    "economic_copyability_budget_usd": "75",
                    "economic_copyability_volume_coverage": "0.82",
                    "economic_copyability_volume_coverage_with_roundup": "0.94",
                }
            ]
        )

        self.assertIn("budget $75", text)
        self.assertIn("vol 82%/94% round", text)

    def test_strict_capital_pruning_reduces_universe_when_bankroll_is_too_small(self) -> None:
        rows = [
            {
                "user_name": "A",
                "wallet": "wallet-a",
                "category": "SPORTS",
                "final_wss": "80",
                "weight": "0.34",
                "economic_copyability_required_bankroll_p95_volume_usd": "400",
            },
            {
                "user_name": "B",
                "wallet": "wallet-b",
                "category": "CULTURE",
                "final_wss": "79",
                "weight": "0.33",
                "economic_copyability_required_bankroll_p95_volume_usd": "800",
            },
            {
                "user_name": "C",
                "wallet": "wallet-c",
                "category": "POLITICS",
                "final_wss": "78",
                "weight": "0.33",
                "economic_copyability_required_bankroll_p95_volume_usd": "1200",
            },
        ]

        pruned, note, summary = rebalance_review._capital_prune_live_rows(
            rows,
            config={
                "capital": {"total_capital_usd": 160.0},
                "economic_copyability": {
                    "capital_aware_rebalance": True,
                    "capital_aware_rebalance_mode": "strict",
                },
            },
        )

        self.assertEqual(len(pruned), 1)
        self.assertEqual(pruned[0]["user_name"], "A")
        self.assertEqual(pruned[0]["weight"], 1.0)
        self.assertIn("reduced proposed universe from 3 to 1", note)
        self.assertEqual(summary["leader_count"], 1)

    def test_balanced_capital_selection_keeps_compromise_universe(self) -> None:
        rows = [
            {
                "user_name": "StrongSmall",
                "wallet": "wallet-a",
                "category": "SPORTS",
                "final_wss": "80",
                "weight": "0.34",
                "economic_copyability_status": "PASS",
                "economic_copyability_buy_signals": "20",
                "economic_copyability_executable_ratio": "0.35",
                "economic_copyability_batchable_ratio": "0.70",
                "economic_copyability_dust_ratio": "0.20",
                "economic_copyability_required_bankroll_p95_volume_usd": "400",
            },
            {
                "user_name": "GoodSmall",
                "wallet": "wallet-b",
                "category": "CULTURE",
                "final_wss": "76",
                "weight": "0.33",
                "economic_copyability_status": "PASS",
                "economic_copyability_buy_signals": "20",
                "economic_copyability_executable_ratio": "0.20",
                "economic_copyability_batchable_ratio": "0.55",
                "economic_copyability_dust_ratio": "0.30",
                "economic_copyability_required_bankroll_p95_volume_usd": "700",
            },
            {
                "user_name": "Huge",
                "wallet": "wallet-c",
                "category": "POLITICS",
                "final_wss": "78",
                "weight": "0.33",
                "economic_copyability_status": "PASS",
                "economic_copyability_buy_signals": "20",
                "economic_copyability_executable_ratio": "0.02",
                "economic_copyability_batchable_ratio": "0.08",
                "economic_copyability_dust_ratio": "0.90",
                "economic_copyability_required_bankroll_p95_volume_usd": "2000",
            },
        ]

        with patch.object(
            rebalance_review,
            "compute_budget_volume_coverage_by_wallet",
            return_value={
                "wallet-a": {
                    "budget_usd": 80.0,
                    "volume_coverage": 0.25,
                    "volume_coverage_with_roundup": 0.55,
                },
                "wallet-b": {
                    "budget_usd": 80.0,
                    "volume_coverage": 0.18,
                    "volume_coverage_with_roundup": 0.42,
                },
                "wallet-c": {
                    "budget_usd": 0.0,
                    "volume_coverage": 0.01,
                    "volume_coverage_with_roundup": 0.05,
                },
            },
        ):
            selected, note, summary = rebalance_review._capital_prune_live_rows(
                rows,
                config={
                    "capital": {"total_capital_usd": 160.0},
                    "economic_copyability": {
                        "capital_aware_rebalance": True,
                        "capital_aware_rebalance_mode": "balanced",
                        "min_live_leaders": 2,
                        "target_live_leaders": 2,
                        "max_live_leaders": 3,
                    },
                },
            )

        self.assertEqual(len(selected), 2)
        self.assertEqual([row["user_name"] for row in selected], ["StrongSmall", "GoodSmall"])
        self.assertIn("balanced selection", note)
        self.assertEqual(summary["leader_count"], 2)
        self.assertGreater(summary["volume_coverage_with_roundup"], 0)

    def test_review_message_includes_capital_pruning_note(self) -> None:
        text = rebalance_review.build_review_message(
            {
                "review_id": "review-1",
                "proposed_live": [
                    {
                        "user_name": "A",
                        "category": "SPORTS",
                        "final_wss": "80",
                        "weight": "1.0",
                    }
                ],
                "capital_pruning_note": "Capital-aware pruning: test note",
                "capital_fit_summary": {
                    "leader_count": 1,
                    "total_capital_usd": 160,
                    "allocated_budget_usd": 160,
                    "known_leaders": 1,
                    "known_budget_usd": 160,
                    "executable_ratio": 0.2,
                    "batchable_ratio": 0.5,
                    "volume_coverage": 0.3,
                    "volume_coverage_with_roundup": 0.6,
                    "estimated_idle_ratio": 0.4,
                },
            }
        )

        self.assertIn("Capital-aware pruning: test note", text)
        self.assertIn("Ожидаемая копируемость", text)
        self.assertIn("после short batch: 50%", text)

    def test_review_message_does_not_report_unknown_copyability_as_zero_coverage(self) -> None:
        text = rebalance_review.build_review_message(
            {
                "review_id": "review-unknown",
                "proposed_live": [
                    {
                        "user_name": "Unknown",
                        "category": "POLITICS",
                        "final_wss": "70",
                        "weight": "1.0",
                    }
                ],
                "capital_pruning_note": (
                    "Capital-aware balanced selection: runtime economic-copyability is unknown"
                ),
                "capital_fit_summary": {
                    "leader_count": 1,
                    "total_capital_usd": 150,
                    "allocated_budget_usd": 150,
                    "known_leaders": 0,
                    "known_budget_usd": 0,
                    "executable_ratio": None,
                    "batchable_ratio": None,
                    "volume_coverage": None,
                    "volume_coverage_with_roundup": None,
                    "estimated_idle_ratio": None,
                    "unknown_leaders": 1,
                },
            }
        )

        self.assertIn(">= min сейчас: n/a", text)
        self.assertIn("после short batch: n/a", text)
        self.assertIn("это WSS-only выбор", text)
        self.assertNotIn("примерно простаивает/dust: 100%", text)

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

            with patch.object(rebalance_review, "PENDING_FILE", pending_json), patch.object(
                rebalance_review,
                "load_executor_config",
                return_value={"economic_copyability": {"capital_aware_rebalance": False}},
            ):
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

            with patch.object(rebalance_review, "PENDING_FILE", pending_json), patch.object(
                rebalance_review,
                "load_executor_config",
                return_value={"economic_copyability": {"capital_aware_rebalance": False}},
            ):
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

            with patch.object(rebalance_review, "PENDING_FILE", pending_json), patch.object(
                rebalance_review,
                "load_executor_config",
                return_value={"economic_copyability": {"capital_aware_rebalance": False}},
            ):
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
