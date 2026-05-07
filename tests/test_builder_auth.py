from __future__ import annotations

import unittest
from unittest.mock import patch

from execution.builder_auth import ExecutorEnv, derive_or_create_api_key
from execution.polymarket_executor import _API_CREDS_CACHE, build_authenticated_client


class BuilderAuthTests(unittest.TestCase):
    def test_derive_or_create_prefers_derive(self) -> None:
        class FakeCreds:
            api_key = "key"

        class FakeClient:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def derive_api_key(self):
                self.calls.append("derive")
                return FakeCreds()

            def create_api_key(self):
                self.calls.append("create")
                return FakeCreds()

        client = FakeClient()
        creds = derive_or_create_api_key(client)

        self.assertEqual(client.calls, ["derive"])
        self.assertEqual(creds.api_key, "key")

    def test_derive_or_create_falls_back_to_create(self) -> None:
        class FakeCreds:
            api_key = "key"

        class FakeClient:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def derive_api_key(self):
                self.calls.append("derive")
                raise RuntimeError("missing")

            def create_api_key(self):
                self.calls.append("create")
                return FakeCreds()

        client = FakeClient()
        creds = derive_or_create_api_key(client)

        self.assertEqual(client.calls, ["derive", "create"])
        self.assertEqual(creds.api_key, "key")

    def test_build_authenticated_client_caches_api_creds(self) -> None:
        class FakeCreds:
            api_key = "key"

        class FakeClient:
            def __init__(self, *args, **kwargs) -> None:
                self.creds = None

            def set_api_creds(self, creds) -> None:
                self.creds = creds

        env = ExecutorEnv(
            clob_host="https://clob.polymarket.com",
            relayer_url="https://relayer-v2.polymarket.com",
            chain_id=137,
            private_key="secret",
            funder_address="0xabc",
            signature_type=1,
            builder_api_key="builder",
            builder_secret="secret",
            builder_passphrase="pass",
        )

        _API_CREDS_CACHE.clear()
        try:
            with patch("execution.polymarket_executor.load_executor_env", return_value=env), \
                 patch("execution.polymarket_executor.ClobClient", FakeClient), \
                 patch("execution.polymarket_executor.derive_or_create_api_key", return_value=FakeCreds()) as derive_mock:
                first = build_authenticated_client()
                second = build_authenticated_client()
        finally:
            _API_CREDS_CACHE.clear()

        self.assertEqual(derive_mock.call_count, 1)
        self.assertEqual(first.creds.api_key, "key")
        self.assertEqual(second.creds.api_key, "key")


if __name__ == "__main__":
    unittest.main()
