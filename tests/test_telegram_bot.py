from __future__ import annotations

import unittest
from unittest.mock import patch

from app import telegram_bot


class TelegramBotTests(unittest.TestCase):
    def test_pick_command_returns_review_inline_markup(self) -> None:
        review = {
            "review_id": "review-1",
            "proposed_live": [
                {
                    "user_name": "cry.eth2",
                    "category": "FINANCE",
                    "final_wss": "66.62",
                    "weight": "1.0",
                }
            ],
        }
        with patch(
            "app.telegram_bot.apply_manual_pick",
            return_value={
                "review": review,
                "chosen": {
                    "user_name": "cry.eth2",
                    "category": "FINANCE",
                    "final_wss": "66.62",
                },
            },
        ):
            response = telegram_bot._handle_pick_command("pick FINANCE 2")

        self.assertIsNotNone(response)
        text, markup = response
        self.assertIn("Выбран cry.eth2", text)
        self.assertIsNotNone(markup)
        buttons = markup["inline_keyboard"]
        self.assertEqual(buttons[0][0]["text"], "Подтвердить")
        self.assertEqual(buttons[0][1]["text"], "Отменить")
        self.assertEqual(buttons[1][0]["text"], "Сменить кандидатов")

    def test_unmarked_command_is_routed(self) -> None:
        with patch("app.telegram_bot.build_unmarked_report", return_value="diag report"):
            response = telegram_bot._build_response("неоцененные", {})

        self.assertEqual(response, "diag report")


if __name__ == "__main__":
    unittest.main()
