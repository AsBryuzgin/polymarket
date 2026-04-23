from __future__ import annotations

import unittest

from collectors.wallet_profiles import WalletProfilesClient


class WalletProfilesClientTests(unittest.TestCase):
    def test_paginate_current_positions_passes_sort_options(self) -> None:
        calls: list[dict] = []

        class FakeClient(WalletProfilesClient):
            def __init__(self) -> None:
                pass

            def get_current_positions(self, **kwargs):  # type: ignore[no-untyped-def]
                calls.append(kwargs)
                return []

        client = FakeClient()
        rows = client.paginate_current_positions(
            "wallet",
            page_size=25,
            max_pages=1,
            sort_by="CURRENT",
            sort_direction="DESC",
        )

        self.assertEqual(rows, [])
        self.assertEqual(calls[0]["user"], "wallet")
        self.assertEqual(calls[0]["limit"], 25)
        self.assertEqual(calls[0]["sort_by"], "CURRENT")
        self.assertEqual(calls[0]["sort_direction"], "DESC")


if __name__ == "__main__":
    unittest.main()
