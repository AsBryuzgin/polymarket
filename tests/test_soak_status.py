from __future__ import annotations

import unittest
from datetime import datetime, timezone

from execution.soak_status import (
    build_paper_soak_status_report,
    flatten_paper_soak_status_report,
)


class PaperSoakStatusTests(unittest.TestCase):
    def _config(self) -> dict:
        return {
            "global": {
                "simulation": True,
                "preview_mode": False,
                "execution_mode": "paper",
            },
            "state": {
                "db_path": "data/executor_state_paper.db",
            },
            "paper_soak": {
                "required_mode": "PAPER",
                "min_hours": 1.0,
                "max_last_event_age_minutes": 30.0,
                "max_event_gap_minutes": 180.0,
                "min_order_attempts": 1,
                "min_processed_signals": 1,
                "min_signal_observations": 1,
                "max_error_attempts": 0,
                "max_unknown_attempts": 0,
                "max_error_signals": 0,
            },
        }

    def test_empty_soak_status(self) -> None:
        report = build_paper_soak_status_report(
            config=self._config(),
            leader_registry_rows=[{"leader_status": "ACTIVE"}],
            open_position_rows=[],
            processed_signal_rows=[],
            order_attempt_rows=[],
            trade_history_rows=[],
            signal_observation_rows=[],
            now=datetime(2026, 4, 18, 12, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(report["soak_status"], "EMPTY")
        self.assertEqual(report["registry"]["leaders"], 1)
        self.assertFalse(report["progress"]["hours"]["ok"])

    def test_ready_for_cutover_check_status(self) -> None:
        report = build_paper_soak_status_report(
            config=self._config(),
            leader_registry_rows=[{"leader_status": "ACTIVE"}],
            open_position_rows=[],
            processed_signal_rows=[
                {
                    "signal_id": "sig1",
                    "status": "PAPER_FILLED_ENTRY",
                    "created_at": "2026-04-18 10:00:00",
                }
            ],
            order_attempt_rows=[
                {
                    "signal_id": "sig1",
                    "status": "PAPER_FILLED",
                    "created_at": "2026-04-18 12:00:00",
                }
            ],
            trade_history_rows=[],
            signal_observation_rows=[
                {
                    "observation_id": 1,
                    "latest_status": "FRESH_COPYABLE",
                    "selected_signal_id": "sig1",
                    "observed_at": "2026-04-18 12:00:00",
                }
            ],
            now=datetime(2026, 4, 18, 12, 10, tzinfo=timezone.utc),
        )

        self.assertEqual(report["soak_status"], "READY_FOR_CUTOVER_CHECK")
        self.assertEqual(report["counts"]["selected_observations"], 1)
        self.assertTrue(report["soak_window"]["last_event_fresh"])

    def test_error_attempt_blocks_status(self) -> None:
        report = build_paper_soak_status_report(
            config=self._config(),
            leader_registry_rows=[{"leader_status": "ACTIVE"}],
            open_position_rows=[],
            processed_signal_rows=[
                {
                    "signal_id": "sig1",
                    "status": "EXECUTION_ERROR",
                    "created_at": "2026-04-18 10:00:00",
                }
            ],
            order_attempt_rows=[
                {
                    "signal_id": "sig1",
                    "status": "EXECUTION_ERROR",
                    "created_at": "2026-04-18 12:00:00",
                }
            ],
            trade_history_rows=[],
            signal_observation_rows=[
                {
                    "observation_id": 1,
                    "latest_status": "FRESH_COPYABLE",
                    "selected_signal_id": "sig1",
                    "observed_at": "2026-04-18 12:00:00",
                }
            ],
            now=datetime(2026, 4, 18, 12, 10, tzinfo=timezone.utc),
        )

        self.assertEqual(report["soak_status"], "BLOCKED")
        self.assertIn("error attempts 1 above allowed 0", report["blockers"])
        self.assertIn("error signals 1 above allowed 0", report["blockers"])

    def test_flatten_status_report_keeps_core_metrics_and_status_counts(self) -> None:
        report = build_paper_soak_status_report(
            config=self._config(),
            leader_registry_rows=[{"leader_status": "ACTIVE"}],
            open_position_rows=[],
            processed_signal_rows=[
                {
                    "signal_id": "sig1",
                    "status": "PAPER_FILLED_ENTRY",
                    "created_at": "2026-04-18 10:00:00",
                }
            ],
            order_attempt_rows=[
                {
                    "signal_id": "sig1",
                    "status": "PAPER_FILLED",
                    "created_at": "2026-04-18 12:00:00",
                }
            ],
            trade_history_rows=[],
            signal_observation_rows=[
                {
                    "observation_id": 1,
                    "latest_status": "FRESH_COPYABLE",
                    "selected_signal_id": "sig1",
                    "observed_at": "2026-04-18 12:00:00",
                }
            ],
            now=datetime(2026, 4, 18, 12, 10, tzinfo=timezone.utc),
        )

        row = flatten_paper_soak_status_report(report)

        self.assertEqual(row["soak_status"], "READY_FOR_CUTOVER_CHECK")
        self.assertEqual(row["leaders"], 1)
        self.assertEqual(row["signal_observations"], 1)
        self.assertEqual(row["observation_status:FRESH_COPYABLE"], 1)
        self.assertEqual(row["processed_signal_status:PAPER_FILLED_ENTRY"], 1)
        self.assertEqual(row["order_attempt_status:PAPER_FILLED"], 1)


if __name__ == "__main__":
    unittest.main()
