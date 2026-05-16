from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from execution.health_check import build_executor_health_report, _processing_age_counts


class ExecutorHealthCheckTests(unittest.TestCase):
    def test_live_without_ack_is_blocked(self) -> None:
        report = build_executor_health_report(
            config={
                "global": {
                    "simulation": False,
                    "preview_mode": False,
                    "execution_mode": "live",
                    "live_trading_enabled": True,
                    "live_trading_ack": "",
                }
            },
            env_health={"env_ok": True},
            open_position_rows=[],
            processed_signal_rows=[],
            order_attempt_rows=[],
            trade_history_rows=[],
            state_db_path="data/executor_state.db",
        )

        self.assertEqual(report["health_status"], "BLOCKED")
        self.assertIn("live trading ack is missing or invalid", report["blockers"])

    def test_recent_processing_is_not_health_warning(self) -> None:
        slow, stuck = _processing_age_counts(
            [
                {
                    "signal_id": "sig1",
                    "status": "PROCESSING",
                    "created_at": "2026-04-18 09:59:30",
                }
            ],
            warning_minutes=2.0,
            critical_minutes=10.0,
            now=datetime(2026, 4, 18, 10, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(slow, 0)
        self.assertEqual(stuck, 0)

    def test_slow_processing_is_warning(self) -> None:
        slow, stuck = _processing_age_counts(
            [
                {
                    "signal_id": "sig1",
                    "status": "PROCESSING",
                    "created_at": "2026-04-18 09:55:00",
                }
            ],
            warning_minutes=2.0,
            critical_minutes=10.0,
            now=datetime(2026, 4, 18, 10, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(slow, 1)
        self.assertEqual(stuck, 0)

    def test_stuck_processing_is_blocker(self) -> None:
        slow, stuck = _processing_age_counts(
            [
                {
                    "signal_id": "sig1",
                    "status": "PROCESSING",
                    "created_at": "2026-04-18 09:45:00",
                }
            ],
            warning_minutes=2.0,
            critical_minutes=10.0,
            now=datetime(2026, 4, 18, 10, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(slow, 0)
        self.assertEqual(stuck, 1)

    def test_recent_processing_does_not_trigger_reconciliation_warning(self) -> None:
        report = build_executor_health_report(
            config={
                "global": {"simulation": True, "preview_mode": True},
                "alerts": {
                    "processing_warning_minutes": 2,
                    "processing_critical_minutes": 10,
                },
            },
            env_health={"env_ok": True},
            open_position_rows=[],
            processed_signal_rows=[
                {
                    "signal_id": "sig1",
                    "leader_wallet": "wallet1",
                    "token_id": "tokenA",
                    "status": "PROCESSING",
                    "reason": "signal claimed",
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            ],
            order_attempt_rows=[],
            trade_history_rows=[],
        )

        self.assertEqual(report["health_status"], "OK")
        self.assertNotIn("reconciliation reported issues", report["warnings"])
        self.assertEqual(report["reconciliation"]["stuck_processing_signals"], 0)

    def test_filled_signal_without_attempt_is_warning(self) -> None:
        report = build_executor_health_report(
            config={"global": {"simulation": True, "preview_mode": True}},
            env_health={"env_ok": True},
            open_position_rows=[],
            processed_signal_rows=[
                {
                    "signal_id": "sig1",
                    "leader_wallet": "wallet1",
                    "token_id": "tokenA",
                    "status": "PREVIEW_READY_ENTRY",
                }
            ],
            order_attempt_rows=[],
            trade_history_rows=[],
        )

        self.assertEqual(report["health_status"], "WARN")
        self.assertIn("reconciliation reported issues", report["warnings"])

    def test_paper_on_default_db_is_blocked_by_runtime_guard(self) -> None:
        report = build_executor_health_report(
            config={
                "global": {
                    "simulation": True,
                    "preview_mode": False,
                    "execution_mode": "paper",
                }
            },
            env_health={"env_ok": True},
            open_position_rows=[],
            processed_signal_rows=[],
            order_attempt_rows=[],
            trade_history_rows=[],
            state_db_path="data/executor_state.db",
        )

        self.assertEqual(report["health_status"], "BLOCKED")
        self.assertIn(
            "PAPER requires an isolated state DB; current DB is the shared default",
            report["blockers"],
        )
        self.assertFalse(report["runtime"]["allowed"])

    def test_live_runtime_lock_is_blocker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            lock_path = Path(tmp) / "runtime.lock"
            lock_path.write_text('{"reason": "critical alert"}', encoding="utf-8")

            report = build_executor_health_report(
                config={
                    "global": {
                        "simulation": False,
                        "preview_mode": False,
                        "execution_mode": "live",
                        "live_trading_enabled": True,
                        "live_trading_ack": "I_UNDERSTAND_THIS_PLACES_REAL_ORDERS",
                    },
                    "runtime_lock": {
                        "enabled": True,
                        "path": str(lock_path),
                    },
                },
                env_health={"env_ok": True},
                open_position_rows=[],
                processed_signal_rows=[],
                order_attempt_rows=[],
                trade_history_rows=[],
                state_db_path="data/executor_state_live.db",
            )

        self.assertEqual(report["health_status"], "BLOCKED")
        self.assertIn("runtime lock active: critical alert", report["blockers"])


if __name__ == "__main__":
    unittest.main()
