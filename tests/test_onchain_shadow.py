from __future__ import annotations

import unittest
from unittest.mock import patch

from eth_abi import encode

from execution.onchain_shadow import (
    ORDER_MATCHED_TOPIC,
    _configured_rpc_urls,
    _decode_orders_matched_log,
    _rpc_url_label,
    poll_onchain_shadow_once,
)


def _topic_address(address: str) -> str:
    return "0x" + address.lower().replace("0x", "").rjust(64, "0")


class OnchainShadowTests(unittest.TestCase):
    def test_uses_clob_v2_orders_matched_topic(self) -> None:
        self.assertEqual(
            ORDER_MATCHED_TOPIC,
            "0x174b3811690657c217184f89418266767c87e4805d09680c39fc9c031c0cab7c",
        )

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
                ["uint8", "uint256", "uint256", "uint256"],
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
                ["uint8", "uint256", "uint256", "uint256"],
                [1, token_id, 3_000_000, 750_000],
            ).hex(),
        }

        decoded = _decode_orders_matched_log(log)

        self.assertIsNotNone(decoded)
        self.assertEqual(decoded.side, "SELL")
        self.assertEqual(decoded.token_id, str(token_id))
        self.assertEqual(decoded.notional_usd, 0.75)
        self.assertEqual(decoded.size, 3.0)
        self.assertEqual(decoded.price, 0.25)

    def test_configured_rpc_urls_uses_env_and_dedupes(self) -> None:
        with patch.dict(
            "os.environ",
            {"POLYGON_RPC_URL": "https://primary.example/rpc, https://backup.example/rpc"},
            clear=False,
        ):
            urls = _configured_rpc_urls(
                {
                    "rpc_url_env": "POLYGON_RPC_URL",
                    "rpc_urls": ["https://backup.example/rpc", "https://config.example/rpc"],
                    "rpc_url": "https://legacy.example/rpc",
                }
            )

        self.assertEqual(urls[0], "https://primary.example/rpc")
        self.assertEqual(urls[1], "https://backup.example/rpc")
        self.assertIn("https://config.example/rpc", urls)
        self.assertIn("https://legacy.example/rpc", urls)
        self.assertEqual(urls.count("https://backup.example/rpc"), 1)

    def test_rpc_label_does_not_expose_path_or_query_secret(self) -> None:
        label = _rpc_url_label("https://polygon-mainnet.g.alchemy.com/v2/secret?token=hidden")

        self.assertEqual(label, "polygon-mainnet.g.alchemy.com")

    def test_poll_uses_fallback_rpc_when_primary_fails(self) -> None:
        def fake_rpc_once(rpc_url, method, params, timeout_sec):
            if rpc_url == "https://bad.example/rpc":
                raise RuntimeError("timeout")
            if method == "eth_blockNumber":
                return hex(110)
            if method == "eth_getLogs":
                return []
            raise AssertionError(method)

        with patch("execution.onchain_shadow.init_onchain_shadow_tables"), \
             patch("execution.onchain_shadow._get_cursor", return_value=100), \
             patch("execution.onchain_shadow._set_cursor") as set_cursor, \
             patch("execution.onchain_shadow._LAST_GOOD_RPC_URL", None), \
             patch("execution.onchain_shadow._rpc_once", side_effect=fake_rpc_once):
            result = poll_onchain_shadow_once(
                {
                    "onchain_shadow": {
                        "enabled": True,
                        "rpc_urls": ["https://bad.example/rpc", "https://good.example/rpc"],
                        "exchange_addresses": ["0xE111180000d2663C0091e4f400237545B87B996B"],
                        "confirmation_blocks": 2,
                        "max_block_range": 100,
                        "startup_backfill_blocks": 5,
                        "timeout_sec": 0.01,
                    }
                },
                leader_wallets=["0x1234567890abcdef1234567890abcdef12345678"],
            )

        self.assertEqual(result["status"], "OK")
        self.assertEqual(result["rpc_successes"], {"good.example": 2})
        self.assertGreaterEqual(result["rpc_failover_count"], 1)
        set_cursor.assert_called_once()

    def test_poll_backs_off_when_head_block_is_ahead_of_logs_rpc(self) -> None:
        def fake_rpc_once(rpc_url, method, params, timeout_sec):
            if method == "eth_blockNumber":
                return hex(120)
            if method == "eth_getLogs":
                request = params[0]
                to_block = int(str(request["toBlock"]), 16)
                if to_block > 105:
                    raise RuntimeError("invalid block range params")
                return []
            raise AssertionError(method)

        with patch("execution.onchain_shadow.init_onchain_shadow_tables"), \
             patch("execution.onchain_shadow._get_cursor", return_value=100), \
             patch("execution.onchain_shadow._set_cursor") as set_cursor, \
             patch("execution.onchain_shadow._LAST_GOOD_RPC_URL", None), \
             patch("execution.onchain_shadow._rpc_once", side_effect=fake_rpc_once):
            result = poll_onchain_shadow_once(
                {
                    "onchain_shadow": {
                        "enabled": True,
                        "rpc_urls": ["https://good.example/rpc"],
                        "exchange_addresses": ["0xE111180000d2663C0091e4f400237545B87B996B"],
                        "confirmation_blocks": 2,
                        "adaptive_backoff_blocks": 13,
                        "max_block_range": 100,
                        "startup_backfill_blocks": 5,
                        "timeout_sec": 0.01,
                    }
                },
                leader_wallets=["0x1234567890abcdef1234567890abcdef12345678"],
            )

        self.assertEqual(result["status"], "OK_BACKOFF")
        self.assertEqual(result["to_block"], 105)
        self.assertEqual(result["target_to_block"], 118)
        set_cursor.assert_called_once()


if __name__ == "__main__":
    unittest.main()
