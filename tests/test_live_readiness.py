from __future__ import annotations

import unittest

from execution.live_readiness import build_live_readiness_report


class LiveReadinessTests(unittest.TestCase):
    def _base_config(self) -> dict:
        return {
            "global": {
                "simulation": True,
                "preview_mode": True,
                "live_trading_enabled": False,
            },
            "funding": {
                "require_balance_allowance": True,
            },
            "live_execution": {
                "require_verified_fill": True,
                "post_submit_poll_attempts": 5,
            },
            "runtime_lock": {
                "enabled": True,
                "activate_on_critical_alerts": True,
            },
            "state_backup": {
                "enabled": True,
            },
            "risk": {
                "min_order_size_usd": 0.01,
                "max_per_trade_pct": 0.05,
                "max_position_pct": 0.08,
                "max_portfolio_exposure_pct": 0.90,
                "max_daily_realized_loss_pct": 0.075,
            },
            "filters": {
                "buy_max_price": 0.96,
            },
            "sizing": {
                "round_up_to_min_order": True,
                "allow_notional_fallback": False,
                "allow_budget_fallback": False,
            },
            "signal_freshness": {
                "max_recent_trades": 50,
                "max_signals_per_cycle": 20,
                "max_price_drift_abs": 0.02,
                "max_price_drift_rel": 0.03,
            },
            "reconciliation": {
                "fetch_exchange_positions": True,
                "fetch_exchange_open_orders": True,
                "position_qty_tolerance": 1e-6,
            },
        }

    def test_live_readiness_blocks_unverified_api_creds(self) -> None:
        report = build_live_readiness_report(
            config=self._base_config(),
            env_health={"env_ok": True, "api_creds_ok": False},
            open_position_rows=[],
            processed_signal_rows=[],
            order_attempt_rows=[],
            trade_history_rows=[],
            exchange_position_rows=[],
            exchange_open_order_rows=[],
            funding_snapshot={"balance_usd": 10.0, "allowance_usd": 2.0},
            state_db_path="data/executor_state_paper.db",
        )

        self.assertEqual(report["readiness_status"], "NO_GO")
        self.assertIn("CLOB API credentials are not verified", report["blockers"])

    def test_live_readiness_blocks_preview_open_positions(self) -> None:
        report = build_live_readiness_report(
            config=self._base_config(),
            env_health={"env_ok": True, "api_creds_ok": True},
            open_position_rows=[
                {
                    "leader_wallet": "wallet1",
                    "token_id": "tokenA",
                    "position_usd": 2.0,
                    "avg_entry_price": 0.5,
                }
            ],
            processed_signal_rows=[],
            order_attempt_rows=[],
            trade_history_rows=[],
            exchange_position_rows=[],
            exchange_open_order_rows=[],
            funding_snapshot={"balance_usd": 10.0, "allowance_usd": 2.0},
            state_db_path="data/executor_state_paper.db",
        )

        self.assertEqual(report["readiness_status"], "NO_GO")
        self.assertIn(
            "preview runtime DB contains open positions; use a clean live DB or reconcile first",
            report["blockers"],
        )

    def test_live_readiness_blocks_partial_live_config(self) -> None:
        config = self._base_config()
        config["global"].update(
            {
                "execution_mode": "live",
                "preview_mode": False,
                "simulation": True,
                "live_trading_enabled": True,
                "live_trading_ack": "",
            }
        )

        report = build_live_readiness_report(
            config=config,
            env_health={"env_ok": True, "api_creds_ok": True},
            open_position_rows=[],
            processed_signal_rows=[],
            order_attempt_rows=[],
            trade_history_rows=[],
            exchange_position_rows=[],
            exchange_open_order_rows=[],
            funding_snapshot={"balance_usd": 10.0, "allowance_usd": 2.0},
            state_db_path="data/executor_state_paper.db",
        )

        self.assertEqual(report["readiness_status"], "NO_GO")
        self.assertIn(
            "execution_mode is live but resolved execution mode is PAPER",
            report["blockers"],
        )
        self.assertIn("live trading ack is missing or invalid", report["blockers"])

    def test_live_readiness_blocks_missing_exchange_snapshots(self) -> None:
        report = build_live_readiness_report(
            config=self._base_config(),
            env_health={"env_ok": True, "api_creds_ok": True},
            open_position_rows=[],
            processed_signal_rows=[],
            order_attempt_rows=[],
            trade_history_rows=[],
        )

        self.assertEqual(report["readiness_status"], "NO_GO")
        self.assertIn("exchange position snapshot was not provided", report["blockers"])
        self.assertIn("exchange open-order snapshot was not provided", report["blockers"])

    def test_live_readiness_can_go_when_all_gates_clean(self) -> None:
        config = self._base_config()
        config["global"]["preview_mode"] = False

        report = build_live_readiness_report(
            config=config,
            env_health={"env_ok": True, "api_creds_ok": True},
            open_position_rows=[],
            processed_signal_rows=[],
            order_attempt_rows=[],
            trade_history_rows=[],
            exchange_position_rows=[],
            exchange_open_order_rows=[],
            funding_snapshot={"balance_usd": 10.0, "allowance_usd": 2.0},
            state_db_path="data/executor_state_paper.db",
        )

        self.assertEqual(report["readiness_status"], "GO")
        self.assertEqual(report["mode"]["resolved_execution_mode"], "PAPER")
        self.assertIn(
            "current config resolves to PAPER; this is a pre-switch readiness check",
            report["warnings"],
        )

    def test_live_readiness_blocks_missing_dynamic_funding_snapshot(self) -> None:
        config = self._base_config()
        config["funding"]["require_positive_balance"] = True
        config["funding"]["min_live_allowance_pct"] = 0.05

        report = build_live_readiness_report(
            config=config,
            env_health={"env_ok": True, "api_creds_ok": True},
            open_position_rows=[],
            processed_signal_rows=[],
            order_attempt_rows=[],
            trade_history_rows=[],
            exchange_position_rows=[],
            exchange_open_order_rows=[],
        )

        self.assertEqual(report["readiness_status"], "NO_GO")
        self.assertIn("funding snapshot was not provided", report["blockers"])
        self.assertIn("funding allowance snapshot was not provided", report["blockers"])

    def test_live_readiness_accepts_sufficient_dynamic_funding(self) -> None:
        config = self._base_config()
        config["funding"]["require_positive_balance"] = True
        config["funding"]["min_live_allowance_pct"] = 0.05

        report = build_live_readiness_report(
            config=config,
            env_health={"env_ok": True, "api_creds_ok": True},
            open_position_rows=[],
            processed_signal_rows=[],
            order_attempt_rows=[],
            trade_history_rows=[],
            exchange_position_rows=[],
            exchange_open_order_rows=[],
            funding_snapshot={"balance_usd": 101.0, "allowance_usd": 5.05},
            state_db_path="data/executor_state_paper.db",
        )

        self.assertEqual(report["readiness_status"], "GO")
        self.assertEqual(report["funding"]["balance_usd"], 101.0)
        self.assertEqual(report["funding"]["min_live_allowance_usd"], 5.05)

    def test_live_readiness_blocks_stale_runtime_config(self) -> None:
        config = self._base_config()
        config["signal_freshness"]["max_recent_trades"] = 3
        config["signal_freshness"]["max_price_drift_abs"] = 0.01

        report = build_live_readiness_report(
            config=config,
            env_health={"env_ok": True, "api_creds_ok": True},
            open_position_rows=[],
            processed_signal_rows=[],
            order_attempt_rows=[],
            trade_history_rows=[],
            exchange_position_rows=[],
            exchange_open_order_rows=[],
            funding_snapshot={"balance_usd": 101.0, "allowance_usd": 5.05},
            state_db_path="data/executor_state_paper.db",
        )

        self.assertEqual(report["readiness_status"], "NO_GO")
        self.assertIn(
            "config safety: signal_freshness.max_recent_trades 3 below required 50",
            report["blockers"],
        )
        self.assertEqual(report["config_safety"]["status"], "NO_GO")


if __name__ == "__main__":
    unittest.main()
