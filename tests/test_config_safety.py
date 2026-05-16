from __future__ import annotations

import unittest

from execution.config_safety import build_config_safety_report


def safe_config() -> dict:
    return {
        "risk": {
            "min_order_size_usd": 0.01,
            "max_per_trade_pct": 0.05,
            "max_position_pct": 0.08,
            "max_portfolio_exposure_pct": 0.90,
            "max_daily_realized_loss_pct": 0.075,
        },
        "filters": {"buy_max_price": 0.96},
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
        "live_execution": {
            "require_verified_fill": True,
            "post_submit_poll_attempts": 5,
        },
        "runtime_lock": {
            "enabled": True,
            "activate_on_critical_alerts": True,
        },
        "state_backup": {"enabled": True},
        "onchain_shadow": {
            "exchange_addresses": [
                "0xE111180000d2663C0091e4f400237545B87B996B",
                "0xe2222d279d744050d28e00520010520000310F59",
            ]
        },
    }


class ConfigSafetyTests(unittest.TestCase):
    def test_safe_runtime_config_goes_green(self) -> None:
        report = build_config_safety_report(safe_config())

        self.assertEqual(report["status"], "GO")
        self.assertEqual(report["blockers"], [])

    def test_stale_burst_and_drift_config_is_blocked(self) -> None:
        config = safe_config()
        config["signal_freshness"].update(
            {
                "max_recent_trades": 3,
                "max_signals_per_cycle": 1,
                "max_price_drift_abs": 0.01,
                "max_price_drift_rel": 0.02,
            }
        )
        config["filters"]["buy_max_price"] = 0.95

        report = build_config_safety_report(config)

        self.assertEqual(report["status"], "NO_GO")
        self.assertIn(
            "signal_freshness.max_recent_trades 3 below required 50",
            report["blockers"],
        )
        self.assertIn(
            "signal_freshness.max_price_drift_abs 0.01 below required 0.02",
            report["blockers"],
        )
        self.assertIn("filters.buy_max_price 0.95 below required 0.96", report["blockers"])

    def test_dangerous_risk_config_is_blocked(self) -> None:
        config = safe_config()
        config["risk"]["max_position_pct"] = 0.20
        config["sizing"]["allow_budget_fallback"] = True
        config["runtime_lock"]["activate_on_critical_alerts"] = False

        report = build_config_safety_report(config)

        self.assertEqual(report["status"], "NO_GO")
        self.assertIn("risk.max_position_pct 0.2 above allowed 0.08", report["blockers"])
        self.assertIn("sizing.allow_budget_fallback must be false", report["blockers"])
        self.assertIn(
            "runtime_lock.activate_on_critical_alerts must be true",
            report["blockers"],
        )

    def test_live_mode_requires_external_alert_delivery(self) -> None:
        config = safe_config()
        config["global"] = {"execution_mode": "live"}
        config["alert_delivery"] = {"enabled": False}

        report = build_config_safety_report(config)

        self.assertEqual(report["status"], "NO_GO")
        self.assertIn("alert_delivery.enabled must be true", report["blockers"])

    def test_v1_exchange_addresses_are_blocked(self) -> None:
        config = safe_config()
        config["onchain_shadow"]["exchange_addresses"] = [
            "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
            "0xe2222d279d744050d28e00520010520000310F59",
        ]

        report = build_config_safety_report(config)

        self.assertEqual(report["status"], "NO_GO")
        self.assertTrue(
            any("deprecated V1 exchange address" in blocker for blocker in report["blockers"])
        )


if __name__ == "__main__":
    unittest.main()
