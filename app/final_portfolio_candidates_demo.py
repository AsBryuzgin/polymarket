from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path


SHORTLIST_DIR = Path("data/shortlists")
CORE_FILE = SHORTLIST_DIR / "master_shortlist_core.csv"
EXPERIMENTAL_FILE = SHORTLIST_DIR / "master_shortlist_experimental.csv"
OUTPUT_FILE = SHORTLIST_DIR / "final_portfolio_candidates.csv"

CORE_QUOTA_PER_CATEGORY = 2
EXPERIMENTAL_QUOTA_PER_CATEGORY = 1
MIN_WSS = 60.0
EPS = 1e-9


def load_csv(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    for row in rows:
        row["leaderboard_pnl"] = float(row["leaderboard_pnl"])
        row["leaderboard_volume"] = float(row["leaderboard_volume"])
        row["final_wss"] = float(row["final_wss"])
        row["raw_wss"] = float(row["raw_wss"])
        row["eligible"] = str(row["eligible"]).lower() == "true"

    return rows


def select_by_category(rows: list[dict], quota_per_category: int) -> list[dict]:
    grouped: dict[str, list[dict]] = defaultdict(list)

    for row in rows:
        if not row["eligible"]:
            continue
        if row["final_wss"] < MIN_WSS:
            continue
        grouped[row["category"]].append(row)

    selected: list[dict] = []

    for category, items in grouped.items():
        items.sort(key=lambda x: x["final_wss"], reverse=True)
        selected.extend(items[:quota_per_category])

    return selected


def deduplicate_wallets(rows: list[dict]) -> list[dict]:
    best_by_wallet: dict[str, dict] = {}

    for row in rows:
        wallet = row["wallet"]

        if wallet not in best_by_wallet:
            best_by_wallet[wallet] = row.copy()
            best_by_wallet[wallet]["all_categories"] = row["category"]
            continue

        existing = best_by_wallet[wallet]
        existing_categories = set(existing["all_categories"].split(", "))
        existing_categories.add(row["category"])
        existing["all_categories"] = ", ".join(sorted(existing_categories))

        current_rank = int(row.get("rank") or 999999)
        existing_rank = int(existing.get("rank") or 999999)
        should_replace = row["final_wss"] > existing["final_wss"] + EPS
        if abs(row["final_wss"] - existing["final_wss"]) <= EPS:
            should_replace = current_rank < existing_rank

        if should_replace:
            row_copy = row.copy()
            row_copy["all_categories"] = existing["all_categories"]
            best_by_wallet[wallet] = row_copy

    result = list(best_by_wallet.values())
    result.sort(key=lambda x: x["final_wss"], reverse=True)
    return result


def save_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        return

    fieldnames = [
        "wallet",
        "user_name",
        "category",
        "all_categories",
        "final_wss",
        "raw_wss",
        "activity_score",
        "leaderboard_pnl",
        "leaderboard_volume",
        "rank",
        "time_period",
        "eligible",
        "filter_reasons",
        "median_spread",
        "median_liquidity",
        "slippage_proxy",
        "current_position_pnl_ratio",
        "trades_30d",
        "trades_90d",
        "days_since_last_trade",
        "closed_positions_used",
    ]

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    core_rows = load_csv(CORE_FILE)
    experimental_rows = load_csv(EXPERIMENTAL_FILE)

    selected_core = select_by_category(core_rows, CORE_QUOTA_PER_CATEGORY)
    selected_experimental = select_by_category(experimental_rows, EXPERIMENTAL_QUOTA_PER_CATEGORY)

    combined = selected_core + selected_experimental
    final_rows = deduplicate_wallets(combined)

    print("=" * 120)
    print("FINAL PORTFOLIO CANDIDATES")
    print("=" * 120)

    for row in final_rows:
        print(
            f"user={row['user_name']} | "
            f"wallet={row['wallet']} | "
            f"category={row['category']} | "
            f"all_categories={row['all_categories']} | "
            f"wss={row['final_wss']}"
        )

    save_csv(final_rows, OUTPUT_FILE)
    print(f"\nSaved: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
