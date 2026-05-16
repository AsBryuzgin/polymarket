from __future__ import annotations

import unittest

from execution.position_marking import is_marked, is_unmarked, mark_position


class PositionMarkingTests(unittest.TestCase):
    def test_resolved_winner_market_uses_settlement_fallback(self) -> None:
        position = {
            "leader_wallet": "wallet1",
            "token_id": "tokenA",
            "position_usd": 5.0,
            "avg_entry_price": 0.50,
        }

        def snapshot_loader(_token_id: str, _side: str):
            raise RuntimeError("No orderbook exists for the requested token id")

        def diagnosis_loader(_token_id: str, _error_message: str):
            return {
                "diagnosis_status": "NO_ORDERBOOK_CLOSED_OR_RESOLVED",
                "diagnosis_label": "closed/resolved",
                "diagnosis_reason": "market resolved",
                "action_hint": "redeem path is needed",
                "question": "Will it rain?",
                "token_outcome": "Yes",
                "token_winner": True,
            }

        row = mark_position(
            position,
            snapshot_loader=snapshot_loader,
            diagnosis_loader=diagnosis_loader,
        )

        self.assertEqual(row["snapshot_status"], "SETTLED")
        self.assertEqual(row["mark_source"], "SETTLEMENT")
        self.assertEqual(row["settlement_price"], 1.0)
        self.assertEqual(row["mark_value_mid_usd"], 10.0)
        self.assertTrue(is_marked(row))
        self.assertFalse(is_unmarked(row))

    def test_non_resolved_no_orderbook_stays_unmarked(self) -> None:
        position = {
            "leader_wallet": "wallet1",
            "token_id": "tokenA",
            "position_usd": 5.0,
            "avg_entry_price": 0.50,
        }

        def snapshot_loader(_token_id: str, _side: str):
            raise RuntimeError("No orderbook exists for the requested token id")

        def diagnosis_loader(_token_id: str, _error_message: str):
            return {
                "diagnosis_status": "NO_ORDERBOOK_ACTIVE_MARKET",
                "diagnosis_label": "active market without book",
                "diagnosis_reason": "suspicious",
                "action_hint": "inspect manually",
            }

        row = mark_position(
            position,
            snapshot_loader=snapshot_loader,
            diagnosis_loader=diagnosis_loader,
        )

        self.assertEqual(row["snapshot_status"], "ERROR")
        self.assertEqual(row["mark_source"], "UNMARKED")
        self.assertIsNone(row["mark_value_mid_usd"])
        self.assertFalse(is_marked(row))
        self.assertTrue(is_unmarked(row))


if __name__ == "__main__":
    unittest.main()
