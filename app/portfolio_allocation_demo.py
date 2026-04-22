from __future__ import annotations

import csv
import tomllib
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path


INPUT_FILE = Path("data/shortlists/final_portfolio_candidates.csv")
OUTPUT_FILE = Path("data/shortlists/final_portfolio_allocation.csv")
REBALANCE_CONFIG = Path("config/rebalance.toml")
EXECUTOR_CONFIG = Path("config/executor.toml")

DEFAULT_MAX_WALLET_WEIGHT = 0.12
DEFAULT_MAX_CATEGORY_WEIGHT = 0.25
DEFAULT_MAX_EXPERIMENTAL_WEIGHT = 0.08
EXPERIMENTAL_CATEGORIES = {"MENTIONS"}

EPS = 1e-12


@dataclass(frozen=True)
class AllocationCaps:
    max_wallet_weight: float
    max_category_weight: float
    max_experimental_weight: float
    wallet_cap_source: str


def load_toml(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open("rb") as f:
        return tomllib.load(f)


def _positive_float(value, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def resolve_allocation_caps(
    *,
    rebalance_config: dict | None = None,
    executor_config: dict | None = None,
) -> AllocationCaps:
    rebalance_config = rebalance_config if rebalance_config is not None else load_toml(REBALANCE_CONFIG)
    executor_config = executor_config if executor_config is not None else load_toml(EXECUTOR_CONFIG)

    portfolio_cfg = executor_config.get("portfolio", {})
    rebalance_cfg = rebalance_config.get("rebalance", {})
    max_live_categories = int(rebalance_cfg.get("max_live_categories", 0) or 0)

    if max_live_categories > 0:
        max_wallet_weight = round(1.0 / max_live_categories, 8)
        wallet_cap_source = f"auto_from_max_live_categories={max_live_categories}"
    else:
        max_wallet_weight = _positive_float(
            portfolio_cfg.get("max_wallet_weight"),
            DEFAULT_MAX_WALLET_WEIGHT,
        )
        wallet_cap_source = "executor.portfolio.max_wallet_weight"

    return AllocationCaps(
        max_wallet_weight=max_wallet_weight,
        max_category_weight=_positive_float(
            portfolio_cfg.get("max_category_weight"),
            DEFAULT_MAX_CATEGORY_WEIGHT,
        ),
        max_experimental_weight=_positive_float(
            portfolio_cfg.get("max_experimental_weight"),
            DEFAULT_MAX_EXPERIMENTAL_WEIGHT,
        ),
        wallet_cap_source=wallet_cap_source,
    )


def load_csv(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    for row in rows:
        row["final_wss"] = float(row["final_wss"])
        row["leaderboard_pnl"] = float(row["leaderboard_pnl"])
        row["leaderboard_volume"] = float(row["leaderboard_volume"])

    return rows


def normalize_raw_weights(rows: list[dict]) -> None:
    total_score = sum(max(row["final_wss"], 0.0) for row in rows)

    if total_score <= 0:
        for row in rows:
            row["raw_weight"] = 0.0
        return

    for row in rows:
        row["raw_weight"] = max(row["final_wss"], 0.0) / total_score


def category_cap(category: str, caps: AllocationCaps) -> float:
    return caps.max_experimental_weight if category in EXPERIMENTAL_CATEGORIES else caps.max_category_weight


def allocate_with_hard_caps(rows: list[dict], caps: AllocationCaps) -> tuple[float, bool]:
    for row in rows:
        row["weight"] = 0.0

    category_used = defaultdict(float)
    remaining_total = 1.0

    # Precompute feasibility upper bound
    max_possible = 0.0
    by_category = defaultdict(list)
    for row in rows:
        by_category[row["category"]].append(row)

    for category, items in by_category.items():
        max_possible += min(
            category_cap(category, caps),
            sum(caps.max_wallet_weight for _ in items),
        )

    if max_possible + EPS < 1.0:
        return remaining_total, False

    # Progressive allocation:
    # repeatedly distribute remaining weight proportional to raw_weight,
    # while respecting wallet and category residual capacities.
    for _ in range(1000):
        if remaining_total <= EPS:
            return 0.0, True

        eligible = []
        for row in rows:
            wallet_remaining = caps.max_wallet_weight - row["weight"]
            cat_remaining = category_cap(row["category"], caps) - category_used[row["category"]]
            if wallet_remaining > EPS and cat_remaining > EPS and row["raw_weight"] > 0:
                eligible.append((row, wallet_remaining, cat_remaining))

        if not eligible:
            break

        total_raw = sum(row["raw_weight"] for row, _, _ in eligible)
        if total_raw <= EPS:
            break

        progress = 0.0

        for row, wallet_remaining, cat_remaining in eligible:
            target_share = remaining_total * (row["raw_weight"] / total_raw)
            alloc = min(target_share, wallet_remaining, cat_remaining)

            if alloc <= EPS:
                continue

            row["weight"] += alloc
            category_used[row["category"]] += alloc
            progress += alloc

        remaining_total -= progress

        if progress <= EPS:
            break

    return remaining_total, remaining_total <= 1e-8


def save_csv(rows: list[dict], path: Path) -> None:
    fieldnames = [
        "user_name",
        "wallet",
        "category",
        "all_categories",
        "final_wss",
        "leaderboard_pnl",
        "leaderboard_volume",
        "raw_weight",
        "weight",
    ]

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for row in rows:
            writer.writerow(
                {
                    "user_name": row["user_name"],
                    "wallet": row["wallet"],
                    "category": row["category"],
                    "all_categories": row["all_categories"],
                    "final_wss": round(row["final_wss"], 2),
                    "leaderboard_pnl": round(row["leaderboard_pnl"], 2),
                    "leaderboard_volume": round(row["leaderboard_volume"], 2),
                    "raw_weight": round(row["raw_weight"], 6),
                    "weight": round(row["weight"], 6),
                }
            )


def print_summary(
    rows: list[dict],
    remaining_total: float,
    feasible: bool,
    caps: AllocationCaps,
) -> None:
    print("=" * 120)
    print("FINAL PORTFOLIO ALLOCATION")
    print("=" * 120)

    category_totals = defaultdict(float)
    for row in rows:
        category_totals[row["category"]] += row["weight"]
        print(
            f"user={row['user_name']} | "
            f"category={row['category']} | "
            f"wss={row['final_wss']:.2f} | "
            f"weight={row['weight']:.4f} | "
            f"wallet={row['wallet']}"
        )

    print("\nCATEGORY TOTALS")
    for category, total in sorted(category_totals.items(), key=lambda x: x[1], reverse=True):
        cap = category_cap(category, caps)
        print(f"{category}: {total:.4f} (cap={cap:.4f})")

    max_wallet = max((row["weight"] for row in rows), default=0.0)
    total_weight = sum(row["weight"] for row in rows)

    print("\nCHECKS")
    print(f"total_weight={total_weight:.6f}")
    print(f"max_wallet_weight_observed={max_wallet:.6f}")
    print(f"max_wallet_weight_cap={caps.max_wallet_weight:.6f} ({caps.wallet_cap_source})")
    print(f"feasible={feasible}")
    print(f"unallocated_weight={remaining_total:.10f}")


def main() -> None:
    caps = resolve_allocation_caps()
    rows = load_csv(INPUT_FILE)
    normalize_raw_weights(rows)

    remaining_total, feasible = allocate_with_hard_caps(rows, caps)

    rows.sort(key=lambda x: x["weight"], reverse=True)

    print_summary(rows, remaining_total, feasible, caps)
    save_csv(rows, OUTPUT_FILE)
    print(f"\nSaved: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
