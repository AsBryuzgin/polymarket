from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from execution.live_safety import evaluate_live_buy_safety
from execution.runtime_lock import clear_runtime_lock, read_runtime_lock


class LiveSafetyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.lock_path = Path(self.tmpdir.name) / "runtime.lock"

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def _live_config(self) -> dict:
        return {
            "global": {
                "simulation": False,
                "preview_mode": False,
                "execution_mode": "live",
            },
            "live_safety": {
                "enable_stop_buy_on_critical": True,
            },
            "runtime_lock": {
                "enabled": True,
                "activate_on_critical_alerts": True,
                "path": str(self.lock_path),
            },
        }

    def test_live_safety_blocks_critical_order_attempt(self) -> None:
        decision = evaluate_live_buy_safety(
            config=self._live_config(),
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

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.mode, "LIVE")
        self.assertEqual(decision.critical_alerts, 1)
        self.assertIn("live stop-buy active", decision.reason)
        self.assertTrue(read_runtime_lock(self._live_config()).locked)

    def test_runtime_lock_blocks_even_without_current_alerts(self) -> None:
        config = self._live_config()
        self.lock_path.write_text('{"reason": "manual stop"}', encoding="utf-8")

        decision = evaluate_live_buy_safety(
            config=config,
            processed_signal_rows=[],
            order_attempt_rows=[],
        )

        self.assertFalse(decision.allowed)
        self.assertIn("runtime lock active", decision.reason)
        clear_runtime_lock(config)
        self.assertFalse(read_runtime_lock(config).locked)

    def test_live_safety_allows_non_live_modes(self) -> None:
        decision = evaluate_live_buy_safety(
            config={
                "global": {
                    "simulation": True,
                    "preview_mode": False,
                    "execution_mode": "paper",
                }
            },
            processed_signal_rows=[],
            order_attempt_rows=[
                {
                    "attempt_id": 1,
                    "signal_id": "sig1",
                    "status": "LIVE_SUBMITTED_UNVERIFIED",
                }
            ],
        )

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.mode, "PAPER")

    def test_live_safety_can_be_disabled_by_config(self) -> None:
        config = self._live_config()
        config["live_safety"]["enable_stop_buy_on_critical"] = False

        decision = evaluate_live_buy_safety(
            config=config,
            processed_signal_rows=[],
            order_attempt_rows=[
                {
                    "attempt_id": 1,
                    "signal_id": "sig1",
                    "status": "LIVE_SUBMITTED_UNVERIFIED",
                }
            ],
        )

        self.assertTrue(decision.allowed)
        self.assertIn("disabled", decision.reason)


if __name__ == "__main__":
    unittest.main()
