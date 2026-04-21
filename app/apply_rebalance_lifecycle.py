from __future__ import annotations

import csv
import sys
import tomllib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from pprint import pprint

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.allocation_runtime import resolve_leader_budget_usd, resolve_total_capital_usd
from execution.builder_auth import load_executor_config
from execution.state_store import (
    init_db,
    list_leader_registry,
    list_open_positions,
    upsert_leader_registry_row,
)


LIVE_FILE = Path("data/shortlists/live_portfolio_allocation.csv")
REBALANCE_CONFIG = Path("config/rebalance.toml")


def load_live_rows(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    for row in rows:
        row["weight"] = float(row["weight"])
        row["final_wss"] = float(row["final_wss"])

    return rows


def load_rebalance_config(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open("rb") as f:
        return tomllib.load(f)


def load_grace_days(cfg: dict) -> int:
    return int(cfg.get("lifecycle", {}).get("exit_grace_days", 14))


def load_total_capital_usd(cfg: dict) -> float:
    return float(cfg.get("capital", {}).get("total_capital_usd", 0.0))


def main() -> None:
    init_db()

    live_rows = load_live_rows(LIVE_FILE)
    executor_cfg = load_executor_config()
    rebalance_cfg = load_rebalance_config(REBALANCE_CONFIG)
    grace_days = load_grace_days(rebalance_cfg)
    total_capital_usd = resolve_total_capital_usd(
        executor_config=executor_cfg,
        rebalance_config=rebalance_cfg,
        allow_zero_collateral_balance=True,
    )
    if total_capital_usd <= 0:
        print(
            "WARNING: account collateral balance is zero; leader registry will be "
            "bootstrapped with target_budget_usd=0.0. Re-run this command after funding "
            "to refresh live budgets from the real account balance."
        )

    now = datetime.now(timezone.utc)
    grace_until = (now + timedelta(days=grace_days)).isoformat()

    desired_wallets = {row["wallet"] for row in live_rows}
    current_registry = {row["wallet"]: row for row in list_leader_registry(limit=500)}
    open_positions = list_open_positions(limit=100000)

    open_position_wallets = {row["leader_wallet"] for row in open_positions}

    report = []

    # Step 1: mark live leaders as ACTIVE
    for row in live_rows:
        wallet = row["wallet"]
        category = row["category"]
        user_name = row["user_name"]
        weight = float(row["weight"])
        budget = resolve_leader_budget_usd(row, total_capital_usd=total_capital_usd)

        previous = current_registry.get(wallet)
        previous_status = previous["leader_status"] if previous else None

        upsert_leader_registry_row(
            wallet=wallet,
            category=category,
            user_name=user_name,
            leader_status="ACTIVE",
            target_weight=weight,
            target_budget_usd=budget,
            grace_until=None,
            source_tag="live_portfolio_allocation",
        )

        report.append({
            "wallet": wallet,
            "category": category,
            "user_name": user_name,
            "previous_status": previous_status,
            "new_status": "ACTIVE",
            "reason": "present in live universe",
            "has_open_position": wallet in open_position_wallets,
        })

    # Step 2: mark dropped leaders as EXIT_ONLY if we know them already
    for wallet, row in current_registry.items():
        if wallet in desired_wallets:
            continue

        previous_status = row["leader_status"]
        if previous_status not in {"ACTIVE", "EXIT_ONLY"}:
            continue

        upsert_leader_registry_row(
            wallet=wallet,
            category=row["category"],
            user_name=row.get("user_name") or "",
            leader_status="EXIT_ONLY",
            target_weight=0.0,
            target_budget_usd=0.0,
            grace_until=grace_until,
            source_tag="rebalanced_out",
        )

        report.append({
            "wallet": wallet,
            "category": row["category"],
            "user_name": row.get("user_name") or "",
            "previous_status": previous_status,
            "new_status": "EXIT_ONLY",
            "reason": f"removed from live universe; grace until {grace_until}",
            "has_open_position": wallet in open_position_wallets,
        })

    print("=== APPLY REBALANCE LIFECYCLE ===")
    pprint(report)
    print("\n=== CURRENT LEADER REGISTRY ===")
    pprint(list_leader_registry(limit=500))


if __name__ == "__main__":
    main()
