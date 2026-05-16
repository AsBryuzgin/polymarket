from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import execution.state_store as state_store
from execution.budget_accounting import (
    compute_active_budget_plan,
    refresh_active_budgets_after_exit_reserve,
    resolve_budget_total_capital_usd,
)


class BudgetAccountingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        state_store.DB_PATH = Path(self.tmpdir.name) / "executor_state.db"
        state_store.init_db()

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_exit_only_open_positions_reduce_active_budget_pool(self) -> None:
        plan = compute_active_budget_plan(
            total_capital_usd=100.0,
            registry_rows=[
                {
                    "wallet": "active-a",
                    "leader_status": "ACTIVE",
                    "target_weight": 0.6,
                    "category": "A",
                },
                {
                    "wallet": "active-b",
                    "leader_status": "ACTIVE",
                    "target_weight": 0.4,
                    "category": "B",
                },
                {
                    "wallet": "exit-a",
                    "leader_status": "EXIT_ONLY",
                    "target_weight": 0.0,
                    "category": "C",
                },
            ],
            open_positions=[
                {
                    "leader_wallet": "exit-a",
                    "token_id": "token-exit",
                    "position_usd": 30.0,
                }
            ],
        )

        self.assertEqual(plan["exit_only_reserved_usd"], 30.0)
        self.assertEqual(plan["active_capital_usd"], 70.0)
        budgets = {row["wallet"]: row["target_budget_usd"] for row in plan["allocations"]}
        self.assertEqual(budgets, {"active-a": 42.0, "active-b": 28.0})

    def test_orphan_open_positions_are_reserved_until_unwound(self) -> None:
        plan = compute_active_budget_plan(
            total_capital_usd=100.0,
            registry_rows=[
                {
                    "wallet": "active-a",
                    "leader_status": "ACTIVE",
                    "target_weight": 1.0,
                },
            ],
            open_positions=[
                {
                    "leader_wallet": "dropped-wallet",
                    "token_id": "token-exit",
                    "position_usd": 12.5,
                }
            ],
        )

        self.assertEqual(plan["exit_only_reserved_usd"], 12.5)
        self.assertEqual(plan["active_capital_usd"], 87.5)
        self.assertEqual(plan["allocations"][0]["target_budget_usd"], 87.5)

    def test_refresh_active_budgets_releases_budget_after_exit_only_position_sells(self) -> None:
        state_store.upsert_leader_registry_row(
            wallet="active-a",
            category="A",
            user_name="Active A",
            leader_status="ACTIVE",
            target_weight=0.6,
            target_budget_usd=60.0,
            grace_until=None,
            source_tag="test",
        )
        state_store.upsert_leader_registry_row(
            wallet="active-b",
            category="B",
            user_name="Active B",
            leader_status="ACTIVE",
            target_weight=0.4,
            target_budget_usd=40.0,
            grace_until=None,
            source_tag="test",
        )
        state_store.upsert_leader_registry_row(
            wallet="exit-a",
            category="C",
            user_name="Exit A",
            leader_status="EXIT_ONLY",
            target_weight=0.0,
            target_budget_usd=0.0,
            grace_until=None,
            source_tag="test",
        )
        state_store.upsert_buy_position(
            leader_wallet="exit-a",
            token_id="token-exit",
            amount_usd=30.0,
            entry_price=0.50,
            signal_id="seed",
        )

        first = refresh_active_budgets_after_exit_reserve(total_capital_usd=100.0)
        self.assertEqual(first["exit_only_reserved_usd"], 30.0)
        self.assertEqual(state_store.get_leader_registry("active-a")["target_budget_usd"], 42.0)
        self.assertEqual(state_store.get_leader_registry("active-b")["target_budget_usd"], 28.0)

        state_store.reduce_or_close_position(
            leader_wallet="exit-a",
            token_id="token-exit",
            signal_id="sell-half",
            amount_usd=10.0,
        )
        second = refresh_active_budgets_after_exit_reserve(total_capital_usd=100.0)

        self.assertEqual(second["exit_only_reserved_usd"], 20.0)
        self.assertEqual(state_store.get_leader_registry("active-a")["target_budget_usd"], 48.0)
        self.assertEqual(state_store.get_leader_registry("active-b")["target_budget_usd"], 32.0)

    def test_collateral_balance_budget_base_can_include_open_positions(self) -> None:
        total = resolve_budget_total_capital_usd(
            executor_config={"capital": {"source": "collateral_balance"}},
            open_positions=[
                {"leader_wallet": "active-a", "position_usd": 15.0},
                {"leader_wallet": "exit-a", "position_usd": 5.0},
            ],
            balance_loader=lambda _config: 80.0,
        )

        self.assertEqual(total, 100.0)


if __name__ == "__main__":
    unittest.main()
