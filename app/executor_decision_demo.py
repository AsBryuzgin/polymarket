from __future__ import annotations

import os
import sys
from pathlib import Path
from pprint import pprint

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.builder_auth import load_executor_config
from execution.copy_sizer import compute_copy_size
from execution.order_policy import evaluate_order_policy
from execution.polymarket_executor import fetch_market_snapshot, preview_market_order

load_dotenv()


def main() -> None:
    config = load_executor_config()

    token_id = os.getenv("PREVIEW_TOKEN_ID", "").strip()
    side = os.getenv("PREVIEW_SIDE", "BUY").strip().upper()
    leader_budget_usd = float(os.getenv("PREVIEW_LEADER_BUDGET_USD", "6.0"))

    if not token_id:
        raise ValueError("PREVIEW_TOKEN_ID is empty in .env")

    risk = config.get("risk", {})
    filters = config.get("filters", {})
    sizing = config.get("sizing", {})

    snapshot = fetch_market_snapshot(token_id=token_id, side=side)

    policy = evaluate_order_policy(
        midpoint=snapshot["midpoint"],
        spread=snapshot["spread"],
        leader_budget_usd=leader_budget_usd,
        min_price=float(filters.get("min_price", 0.10)),
        max_price=float(filters.get("max_price", 0.90)),
        max_spread=float(risk.get("skip_if_spread_gt", 0.03)),
        min_order_size_usd=float(risk.get("min_order_size_usd", 1.0)),
    )

    size = compute_copy_size(
        leader_budget_usd=leader_budget_usd,
        target_trade_fraction=float(sizing.get("target_trade_fraction", 0.20)),
        min_order_size_usd=float(risk.get("min_order_size_usd", 1.0)),
        max_per_trade_usd=float(risk.get("max_per_trade_usd", 2.0)),
    )

    result = {
        "leader_budget_usd": leader_budget_usd,
        "market_snapshot": snapshot,
        "policy_allowed": policy.allowed,
        "policy_reason": policy.reason,
        "size_allowed": size.allowed,
        "size_reason": size.reason,
        "suggested_amount_usd": size.amount_usd,
    }

    if policy.allowed and size.allowed:
        result["preview_order"] = preview_market_order(
            token_id=token_id,
            amount_usd=size.amount_usd,
            side=side,
        )

    print("=== Executor Decision Demo ===")
    pprint(result)


if __name__ == "__main__":
    main()
