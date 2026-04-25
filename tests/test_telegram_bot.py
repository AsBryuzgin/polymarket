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

    def test_settlements_command_is_routed(self) -> None:
        with patch("app.telegram_bot.build_settlements_report", return_value="settlement report"):
            response = telegram_bot._build_response("сеттлмент", {})

        self.assertEqual(response, "settlement report")

    def test_latency_command_is_routed(self) -> None:
        with patch("app.telegram_bot.build_latency_report", return_value="latency report"):
            response = telegram_bot._build_response("latency", {})

        self.assertEqual(response, "latency report")

    def test_unwind_selection_markup_lists_all_and_leaders(self) -> None:
        with patch(
            "app.telegram_bot.list_unwind_targets",
            return_value=[
                {
                    "wallet": "wallet1",
                    "user_name": "Leader",
                    "category": "SPORTS",
                    "positions": 2,
                    "position_usd": 5.5,
                }
            ],
        ):
            text = telegram_bot._build_unwind_selection_text({"global": {"execution_mode": "PAPER"}})
            markup = telegram_bot._unwind_selection_markup()

        self.assertIn("Ручной выход по рынку", text)
        self.assertEqual(markup["inline_keyboard"][0][0]["callback_data"], "unwind_select:ALL")
        self.assertEqual(markup["inline_keyboard"][1][0]["callback_data"], "unwind_select:wallet1")

    def test_unwind_confirm_text_uses_preview(self) -> None:
        with patch(
            "app.telegram_bot.build_unwind_preview",
            return_value={
                "leaders": 1,
                "positions": 2,
                "position_usd": 5.5,
                "leader_names": ["Leader"],
            },
        ), patch(
            "app.telegram_bot.list_unwind_targets",
            return_value=[
                {
                    "wallet": "wallet1",
                    "user_name": "Leader",
                    "category": "SPORTS",
                    "positions": 2,
                    "position_usd": 5.5,
                }
            ],
        ):
            text = telegram_bot._build_unwind_confirm_text("wallet1")
            markup = telegram_bot._unwind_confirm_markup("wallet1")

        self.assertIn("Подтвердить рыночный выход", text)
        self.assertIn("Leader | SPORTS", text)
        self.assertEqual(markup["inline_keyboard"][0][0]["callback_data"], "unwind_confirm:wallet1")


if __name__ == "__main__":
    unittest.main()
