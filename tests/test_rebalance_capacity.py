from __future__ import annotations

import unittest
import tempfile
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import app.build_live_universe_stable as stable_universe
from app.build_live_universe_stable import apply_live_category_capacity, load_csv
from app.portfolio_allocation_demo import resolve_allocation_caps


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

    def test_allocation_wallet_cap_tracks_live_category_capacity(self) -> None:
        caps = resolve_allocation_caps(
            rebalance_config={"rebalance": {"max_live_categories": 8}},
            executor_config={"portfolio": {"max_wallet_weight": 0.25}},
        )

        self.assertEqual(caps.max_wallet_weight, 0.125)
        self.assertEqual(caps.wallet_cap_source, "auto_from_max_live_categories=8")

    def test_allocation_wallet_cap_falls_back_to_config_without_live_capacity(self) -> None:
        caps = resolve_allocation_caps(
            rebalance_config={"rebalance": {"max_live_categories": 0}},
            executor_config={"portfolio": {"max_wallet_weight": 0.20}},
        )

        self.assertEqual(caps.max_wallet_weight, 0.20)

    def test_stable_universe_refreshes_kept_incumbent_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            final_file = root / "final.csv"
            current_file = root / "current.csv"
            output_file = root / "live.csv"
            report_file = root / "report.csv"
            state_file = root / "state.json"
            config_file = root / "rebalance.toml"

            final_file.write_text(
                "\n".join(
                    [
                        "user_name,wallet,category,final_wss,weight,leaderboard_pnl,leaderboard_volume",
                        "FreshLeader,wallet1,ECONOMICS,70.0,0.10,1.0,2.0",
                    ]
                ),
                encoding="utf-8",
            )
            current_file.write_text(
                "\n".join(
                    [
                        "user_name,wallet,category,final_wss,weight,leaderboard_pnl,leaderboard_volume",
                        "OldLeader,wallet1,ECONOMICS,60.0,0.90,1.0,2.0",
                    ]
                ),
                encoding="utf-8",
            )
            config_file.write_text(
                "[rebalance]\nmax_live_categories = 0\nconfirmation_cycles = 2\n",
                encoding="utf-8",
            )

            with (
                patch.object(stable_universe, "FINAL_FILE", final_file),
                patch.object(stable_universe, "CURRENT_LIVE_FILE", current_file),
                patch.object(stable_universe, "OUTPUT_LIVE_FILE", output_file),
                patch.object(stable_universe, "REPORT_FILE", report_file),
                patch.object(stable_universe, "STATE_FILE", state_file),
                patch.object(stable_universe, "CONFIG_FILE", config_file),
                redirect_stdout(StringIO()),
            ):
                stable_universe.main()

            rows = load_csv(output_file)

        self.assertEqual(rows[0]["user_name"], "FreshLeader")
        self.assertEqual(rows[0]["final_wss"], 70.0)
        self.assertEqual(rows[0]["weight"], 1.0)


if __name__ == "__main__":
    unittest.main()
