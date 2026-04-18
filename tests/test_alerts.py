from __future__ import annotations

import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from execution.alert_delivery import deliver_alerts, format_alert_message
from execution.alerts import build_executor_alerts, has_critical_alerts


class ExecutorAlertsTests(unittest.TestCase):
    def test_unverified_live_order_is_critical_alert(self) -> None:
        alerts = build_executor_alerts(
            config={},
            processed_signal_rows=[],
            order_attempt_rows=[
                {
                    "attempt_id": 1,
                    "signal_id": "sig1",
                    "leader_wallet": "wallet1",
                    "token_id": "tokenA",
                    "side": "BUY",
                    "status": "LIVE_SUBMITTED_UNVERIFIED",
                }
            ],
        )

        self.assertTrue(has_critical_alerts(alerts))
        self.assertEqual(alerts[0]["severity"], "CRITICAL")
        self.assertEqual(alerts[0]["alert_type"], "ORDER_LIVE_SUBMITTED_UNVERIFIED")

    def test_processing_signal_escalates_by_age(self) -> None:
        alerts = build_executor_alerts(
            config={
                "alerts": {
                    "processing_warning_minutes": 2.0,
                    "processing_critical_minutes": 10.0,
                }
            },
            processed_signal_rows=[
                {
                    "signal_id": "sig-stuck",
                    "leader_wallet": "wallet1",
                    "token_id": "tokenA",
                    "status": "PROCESSING",
                    "created_at": "2026-04-18 09:45:00",
                }
            ],
            order_attempt_rows=[],
            now=datetime(2026, 4, 18, 10, 0, tzinfo=timezone.utc),
        )

        self.assertTrue(has_critical_alerts(alerts))
        self.assertEqual(alerts[0]["alert_type"], "SIGNAL_STUCK_PROCESSING")

    def test_health_blocker_becomes_critical_alert(self) -> None:
        alerts = build_executor_alerts(
            config={},
            processed_signal_rows=[],
            order_attempt_rows=[],
            health_report={
                "health_status": "BLOCKED",
                "blockers": ["live trading ack is missing or invalid"],
                "warnings": [],
            },
        )

        self.assertEqual(alerts[0]["alert_type"], "EXECUTOR_HEALTH_BLOCKED")
        self.assertTrue(has_critical_alerts(alerts))

    def test_alert_message_is_compact(self) -> None:
        message = format_alert_message(
            [
                {
                    "severity": "CRITICAL",
                    "alert_type": "ORDER_LIVE_SUBMITTED_UNVERIFIED",
                    "message": "needs review",
                    "signal_id": "sig1",
                }
            ],
            title="Bot alert",
        )

        self.assertIn("Bot alert", message)
        self.assertIn("ORDER_LIVE_SUBMITTED_UNVERIFIED", message)
        self.assertIn("sig1", message)

    def test_deliver_alerts_posts_to_configured_webhooks(self) -> None:
        calls = []

        def fake_post(url, payload):
            calls.append((url, payload))

        config = {
            "alert_delivery": {
                "enabled": True,
                "telegram_bot_token_env": "TEST_TG_TOKEN",
                "telegram_chat_id_env": "TEST_TG_CHAT",
                "discord_webhook_url_env": "TEST_DISCORD_URL",
                "email_webhook_url_env": "TEST_EMAIL_URL",
            }
        }
        env = {
            "TEST_TG_TOKEN": "token",
            "TEST_TG_CHAT": "chat",
            "TEST_DISCORD_URL": "https://discord.example/webhook",
            "TEST_EMAIL_URL": "https://email.example/webhook",
        }

        with patch.dict("os.environ", env):
            results = deliver_alerts(
                config=config,
                alerts=[
                    {
                        "severity": "CRITICAL",
                        "alert_type": "SIGNAL_EXECUTION_ERROR",
                        "message": "boom",
                    }
                ],
                post_json=fake_post,
            )

        self.assertEqual(len(calls), 3)
        self.assertTrue(all(result.delivered for result in results))


if __name__ == "__main__":
    unittest.main()
