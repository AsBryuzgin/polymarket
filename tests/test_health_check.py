from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from execution.health_check import build_executor_health_report


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

    def test_stuck_processing_is_warning(self) -> None:
        report = build_executor_health_report(
            config={"global": {"simulation": True, "preview_mode": True}},
            env_health={"env_ok": False},
            open_position_rows=[],
            processed_signal_rows=[
                {
                    "signal_id": "sig1",
                    "leader_wallet": "wallet1",
                    "token_id": "tokenA",
                    "status": "PROCESSING",
                }
            ],
            order_attempt_rows=[],
            trade_history_rows=[],
        )

        self.assertEqual(report["health_status"], "WARN")
        self.assertEqual(report["state"]["processed_status_counts"]["PROCESSING"], 1)

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
