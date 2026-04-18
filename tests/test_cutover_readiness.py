from __future__ import annotations

import unittest
from datetime import datetime, timezone

from execution.cutover_readiness import build_cutover_readiness_report


class CutoverReadinessTests(unittest.TestCase):
    def _base_config(self) -> dict:
        return {
            "global": {
                "simulation": True,
                "preview_mode": False,
                "execution_mode": "paper",
                "live_trading_enabled": False,
            },
            "state": {
                "db_path": "data/executor_state_paper.db",
            },
            "funding": {
                "require_balance_allowance": True,
            },
            "live_execution": {
                "require_verified_fill": True,
            },
            "paper_soak": {
                "required_mode": "PAPER",
                "min_hours": 1.0,
                "max_last_event_age_minutes": 30.0,
                "max_event_gap_minutes": 120.0,
                "min_order_attempts": 1,
                "min_processed_signals": 1,
                "min_signal_observations": 1,
                "max_error_attempts": 0,
                "max_unknown_attempts": 0,
                "max_error_signals": 0,
                "require_live_readiness": True,
                "require_isolated_db": True,
            },
            "reconciliation": {
                "fetch_exchange_positions": True,
                "fetch_exchange_open_orders": True,
                "position_qty_tolerance": 1e-6,
            },
        }

    def test_cutover_go_when_soak_and_live_readiness_are_clean(self) -> None:
        now = datetime(2026, 4, 17, 12, 0, tzinfo=timezone.utc)
        report = build_cutover_readiness_report(
            config=self._base_config(),
            env_health={"env_ok": True, "api_creds_ok": True},
            open_position_rows=[],
            processed_signal_rows=[
                {
                    "signal_id": "sig1",
                    "status": "PAPER_FILLED_ENTRY",
                    "created_at": "2026-04-17 10:00:00",
                }
            ],
            order_attempt_rows=[
                {
                    "signal_id": "sig1",
                    "status": "PAPER_FILLED",
                    "created_at": "2026-04-17 12:00:00",
                }
            ],
            trade_history_rows=[
                {
                    "event_time": "2026-04-17 11:00:00",
                    "leader_wallet": "wallet1",
                    "token_id": "tokenA",
                    "side": "BUY",
                    "event_type": "ENTRY",
                    "amount_usd": 1.0,
                    "price": 0.5,
                },
                {
                    "event_time": "2026-04-17 11:30:00",
                    "leader_wallet": "wallet1",
                    "token_id": "tokenA",
                    "side": "SELL",
                    "event_type": "EXIT",
                    "amount_usd": 1.0,
                    "price": 0.6,
                }
            ],
            signal_observation_rows=[
                {
                    "observation_id": 1,
                    "observed_at": "2026-04-17 12:00:00",
                    "latest_status": "FRESH_COPYABLE",
                }
            ],
            exchange_position_rows=[],
            exchange_open_order_rows=[],
            funding_snapshot={"balance_usd": 10.0, "allowance_usd": 2.0},
            now=now,
        )

        self.assertEqual(report["cutover_status"], "GO")
        self.assertEqual(report["soak"]["resolved_mode"], "PAPER")

    def test_cutover_blocks_preview_mode_and_short_soak(self) -> None:
        config = self._base_config()
        config["global"]["preview_mode"] = True
        now = datetime(2026, 4, 17, 12, 0, tzinfo=timezone.utc)

        report = build_cutover_readiness_report(
            config=config,
            env_health={"env_ok": True, "api_creds_ok": True},
            open_position_rows=[],
            processed_signal_rows=[
                {"signal_id": "sig1", "status": "PREVIEW_READY_ENTRY", "created_at": "2026-04-17 12:00:00"}
            ],
            order_attempt_rows=[
                {"signal_id": "sig1", "status": "PREVIEW_READY", "created_at": "2026-04-17 12:00:00"}
            ],
            trade_history_rows=[],
            signal_observation_rows=[{"observation_id": 1, "observed_at": "2026-04-17 12:00:00"}],
            exchange_position_rows=[],
            exchange_open_order_rows=[],
            now=now,
        )

        self.assertEqual(report["cutover_status"], "NO_GO")
        self.assertIn("soak must run in PAPER; current mode is PREVIEW", report["blockers"])
        self.assertTrue(any("soak window" in blocker for blocker in report["blockers"]))

    def test_cutover_blocks_stale_last_event(self) -> None:
        now = datetime(2026, 4, 17, 12, 0, tzinfo=timezone.utc)
        report = build_cutover_readiness_report(
            config=self._base_config(),
            env_health={"env_ok": True, "api_creds_ok": True},
            open_position_rows=[],
            processed_signal_rows=[
                {"signal_id": "sig1", "status": "PAPER_FILLED_ENTRY", "created_at": "2026-04-17 09:00:00"}
            ],
            order_attempt_rows=[
                {"signal_id": "sig1", "status": "PAPER_FILLED", "created_at": "2026-04-17 10:00:00"}
            ],
            trade_history_rows=[],
            signal_observation_rows=[{"observation_id": 1, "observed_at": "2026-04-17 10:00:00"}],
            exchange_position_rows=[],
            exchange_open_order_rows=[],
            now=now,
        )

        self.assertEqual(report["cutover_status"], "NO_GO")
        self.assertTrue(any("last soak event age" in blocker for blocker in report["blockers"]))

    def test_cutover_blocks_shared_default_state_db(self) -> None:
        config = self._base_config()
        config["state"]["db_path"] = "data/executor_state.db"
        now = datetime(2026, 4, 17, 12, 0, tzinfo=timezone.utc)

        report = build_cutover_readiness_report(
            config=config,
            env_health={"env_ok": True, "api_creds_ok": True},
            open_position_rows=[],
            processed_signal_rows=[
                {"signal_id": "sig1", "status": "PAPER_FILLED_ENTRY", "created_at": "2026-04-17 10:00:00"}
            ],
            order_attempt_rows=[
                {"signal_id": "sig1", "status": "PAPER_FILLED", "created_at": "2026-04-17 12:00:00"}
            ],
            trade_history_rows=[
                {
                    "event_time": "2026-04-17 11:00:00",
                    "leader_wallet": "wallet1",
                    "token_id": "tokenA",
                    "side": "BUY",
                    "event_type": "ENTRY",
                    "amount_usd": 1.0,
                    "price": 0.5,
                },
                {
                    "event_time": "2026-04-17 11:30:00",
                    "leader_wallet": "wallet1",
                    "token_id": "tokenA",
                    "side": "SELL",
                    "event_type": "EXIT",
                    "amount_usd": 1.0,
                    "price": 0.6,
                },
            ],
            signal_observation_rows=[{"observation_id": 1, "observed_at": "2026-04-17 12:00:00"}],
            exchange_position_rows=[],
            exchange_open_order_rows=[],
            now=now,
        )

        self.assertEqual(report["cutover_status"], "NO_GO")
        self.assertIn(
            "paper soak uses the shared default state DB; set POLY_EXECUTOR_DB_PATH "
            "or [state].db_path to an isolated paper DB",
            report["blockers"],
        )

    def test_cutover_blocks_sparse_soak_window(self) -> None:
        config = self._base_config()
        config["paper_soak"]["max_event_gap_minutes"] = 30.0
        now = datetime(2026, 4, 17, 12, 0, tzinfo=timezone.utc)

        report = build_cutover_readiness_report(
            config=config,
            env_health={"env_ok": True, "api_creds_ok": True},
            open_position_rows=[],
            processed_signal_rows=[
                {"signal_id": "sig1", "status": "PAPER_FILLED_ENTRY", "created_at": "2026-04-17 10:00:00"}
            ],
            order_attempt_rows=[
                {"signal_id": "sig1", "status": "PAPER_FILLED", "created_at": "2026-04-17 12:00:00"}
            ],
            trade_history_rows=[],
            signal_observation_rows=[{"observation_id": 1, "observed_at": "2026-04-17 12:00:00"}],
            exchange_position_rows=[],
            exchange_open_order_rows=[],
            now=now,
        )

        self.assertEqual(report["cutover_status"], "NO_GO")
        self.assertTrue(any("max soak event gap" in blocker for blocker in report["blockers"]))

    def test_cutover_blocks_execution_errors(self) -> None:
        now = datetime(2026, 4, 17, 12, 0, tzinfo=timezone.utc)
        report = build_cutover_readiness_report(
            config=self._base_config(),
            env_health={"env_ok": True, "api_creds_ok": True},
            open_position_rows=[],
            processed_signal_rows=[
                {"signal_id": "sig1", "status": "EXECUTION_ERROR", "created_at": "2026-04-17 10:00:00"}
            ],
            order_attempt_rows=[
                {"signal_id": "sig1", "status": "EXECUTION_ERROR", "created_at": "2026-04-17 12:00:00"}
            ],
            trade_history_rows=[],
            signal_observation_rows=[{"observation_id": 1, "observed_at": "2026-04-17 12:00:00"}],
            exchange_position_rows=[],
            exchange_open_order_rows=[],
            now=now,
        )

        self.assertEqual(report["cutover_status"], "NO_GO")
        self.assertIn("error order attempts 1 above allowed 0", report["blockers"])
        self.assertIn("execution error signals 1 above allowed 0", report["blockers"])


if __name__ == "__main__":
    unittest.main()
