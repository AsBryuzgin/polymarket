from __future__ import annotations

import csv
import tomllib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from pprint import pprint

from execution.state_store import (
    get_open_position,
    get_leader_registry,
    init_db,
    list_leader_registry,
    upsert_leader_registry_row,
)


LIVE_FILE = Path("data/shortlists/live_portfolio_allocation.csv")
REBALANCE_CONFIG = Path("config/rebalance.toml")
TOTAL_CAPITAL_USD = 100.0


def load_live_rows(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    for row in rows:
        row["weight"] = float(row["weight"])
        row["final_wss"] = float(row["final_wss"])

    return rows


def load_grace_days(path: Path) -> int:
    if not path.exists():
        return 14
    with path.open("rb") as f:
        cfg = tomllib.load(f)
    return int(cfg.get("lifecycle", {}).get("exit_grace_days", 14))


def main() -> None:
    init_db()

    live_rows = load_live_rows(LIVE_FILE)
    grace_days = load_grace_days(REBALANCE_CONFIG)

    now = datetime.now(timezone.utc)
    grace_until = (now + timedelta(days=grace_days)).isoformat()

    desired_wallets = {row["wallet"] for row in live_rows}
    current_registry = {row["wallet"]: row for row in list_leader_registry(limit=500)}

    report = []

    # Step 1: mark live leaders as ACTIVE
    for row in live_rows:
        wallet = row["wallet"]
        category = row["category"]
        user_name = row["user_name"]
        weight = float(row["weight"])
        budget = round(TOTAL_CAPITAL_USD * weight, 2)

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

        has_any_position = False
        # lightweight check: any open position for this leader?
        # we only know exact rows per token, so use stored registry transition
        # position presence will be enforced later by copy_worker on SELL
        # still useful in report
        has_any_position = get_open_position(wallet, "__dummy__") is not None

        report.append({
            "wallet": wallet,
            "category": row["category"],
            "user_name": row.get("user_name") or "",
            "previous_status": previous_status,
            "new_status": "EXIT_ONLY",
            "reason": f"removed from live universe; grace until {grace_until}",
            "has_position_hint": has_any_position,
        })

    print("=== APPLY REBALANCE LIFECYCLE ===")
    pprint(report)
    print("\n=== CURRENT LEADER REGISTRY ===")
    pprint(list_leader_registry(limit=500))


if __name__ == "__main__":
    main()
