from __future__ import annotations

import unittest

from eth_abi import encode

from execution.onchain_shadow import ORDER_MATCHED_TOPIC, _decode_orders_matched_log


def _topic_address(address: str) -> str:
    return "0x" + address.lower().replace("0x", "").rjust(64, "0")


class OnchainShadowTests(unittest.TestCase):
    def test_decodes_buy_orders_matched_log(self) -> None:
        leader = "0x1234567890abcdef1234567890abcdef12345678"
        token_id = 12345
        log = {
            "topics": [
                ORDER_MATCHED_TOPIC,
                "0x" + ("11" * 32),
                _topic_address(leader),
            ],
            "data": "0x"
            + encode(
                ["uint256", "uint256", "uint256", "uint256"],
                [0, token_id, 2_000_000, 4_000_000],
            ).hex(),
        }

        decoded = _decode_orders_matched_log(log)

        self.assertIsNotNone(decoded)
        self.assertEqual(decoded.leader_wallet, leader.lower())
        self.assertEqual(decoded.side, "BUY")
        self.assertEqual(decoded.token_id, str(token_id))
        self.assertEqual(decoded.notional_usd, 2.0)
        self.assertEqual(decoded.size, 4.0)
        self.assertEqual(decoded.price, 0.5)

    def test_decodes_sell_orders_matched_log(self) -> None:
        leader = "0x1234567890abcdef1234567890abcdef12345678"
        token_id = 67890
        log = {
            "topics": [
                ORDER_MATCHED_TOPIC,
                "0x" + ("22" * 32),
                _topic_address(leader),
            ],
            "data": "0x"
            + encode(
                ["uint256", "uint256", "uint256", "uint256"],
                [token_id, 0, 3_000_000, 750_000],
            ).hex(),
        }

        decoded = _decode_orders_matched_log(log)

        self.assertIsNotNone(decoded)
        self.assertEqual(decoded.side, "SELL")
        self.assertEqual(decoded.token_id, str(token_id))
        self.assertEqual(decoded.notional_usd, 0.75)
        self.assertEqual(decoded.size, 3.0)
        self.assertEqual(decoded.price, 0.25)


if __name__ == "__main__":
    unittest.main()

