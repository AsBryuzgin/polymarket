from __future__ import annotations

import unittest

from execution.trade_notifications import format_trade_notification


class TradeNotificationTests(unittest.TestCase):
    def test_entry_notification_contains_trade_context(self) -> None:
        message = format_trade_notification(
            mode="PAPER",
            event_type="ENTRY",
            leader_wallet="0x1234567890abcdef",
            leader_user_name="Leader",
            category="CRYPTO",
            token_id="token-1234567890abcdef",
            amount_usd=1.85,
            price=0.37,
            position_before_usd=0.0,
            position_after_usd=1.85,
            signal_id="sig1",
        )

        self.assertIn("Polymarket PAPER BUY", message)
        self.assertIn("Leader", message)
        self.assertIn("$1.85", message)
        self.assertIn("sig1", message)

    def test_exit_notification_contains_pnl(self) -> None:
        message = format_trade_notification(
            mode="LIVE",
            event_type="EXIT",
            leader_wallet="0x1234567890abcdef",
            leader_user_name=None,
            category="SPORTS",
            token_id="tokenA",
            amount_usd=5.0,
            price=0.55,
            position_before_usd=5.0,
            position_after_usd=0.0,
            signal_id="sig2",
            realized_pnl_usd=0.5,
            realized_pnl_pct=0.10,
            holding_minutes=42.0,
            closed_fully=True,
        )

        self.assertIn("Polymarket LIVE SELL", message)
        self.assertIn("полный выход", message)
        self.assertIn("+$0.50", message)
        self.assertIn("+10.00%", message)


if __name__ == "__main__":
    unittest.main()
