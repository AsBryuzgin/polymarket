from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.allocation_runtime import resolve_total_capital_usd
from execution.builder_auth import EXECUTOR_CONFIG_ENV_VAR, load_executor_config
from execution.order_router import resolve_execution_mode
from execution.state_store import CONFIG_PATH_ENV_VAR, resolve_state_db_path


class ConfigRuntimeTests(unittest.TestCase):
    def test_executor_config_path_env_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "executor.paper.toml"
            config_path.write_text(
                "[global]\nexecution_mode = 'paper'\n",
                encoding="utf-8",
            )

            with patch.dict("os.environ", {EXECUTOR_CONFIG_ENV_VAR: str(config_path)}):
                cfg = load_executor_config()

        self.assertEqual(cfg["global"]["execution_mode"], "paper")

    def test_state_db_path_uses_executor_config_env_when_db_env_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "executor.paper.toml"
            config_path.write_text(
                "[state]\ndb_path = 'data/executor_state_paper.db'\n",
                encoding="utf-8",
            )

            db_path = resolve_state_db_path(
                env={CONFIG_PATH_ENV_VAR: str(config_path)},
            )

        self.assertEqual(db_path, Path("data/executor_state_paper.db"))

    def test_live_config_uses_isolated_db_and_resolves_live(self) -> None:
        cfg = load_executor_config("config/executor.live.toml")

        self.assertEqual(resolve_execution_mode(cfg), "LIVE")
        self.assertEqual(cfg["state"]["db_path"], "data/executor_state_live.db")
        self.assertFalse(cfg["global"]["live_trading_enabled"])
        self.assertEqual(cfg["capital"]["source"], "collateral_balance")
        self.assertNotIn("total_capital_usd", cfg["capital"])
        self.assertEqual(cfg["risk"]["max_per_trade_pct"], 0.05)
        self.assertNotIn("max_per_trade_usd", cfg["risk"])
        self.assertEqual(cfg["funding"]["min_live_allowance_pct"], 0.05)
        self.assertNotIn("min_live_balance_usd", cfg["funding"])

    def test_capital_source_can_use_collateral_balance(self) -> None:
        capital = resolve_total_capital_usd(
            executor_config={"capital": {"source": "collateral_balance"}},
            rebalance_config={},
            balance_loader=lambda _config: 73.42,
        )

        self.assertEqual(capital, 73.42)


if __name__ == "__main__":
    unittest.main()
