from __future__ import annotations

import csv
from pathlib import Path
from collections import defaultdict


INPUT_FILE = Path("data/shortlists/final_portfolio_candidates.csv")
OUTPUT_FILE = Path("data/shortlists/final_portfolio_allocation.csv")

MAX_WALLET_WEIGHT = 0.12
MAX_CATEGORY_WEIGHT = 0.25
MAX_EXPERIMENTAL_WEIGHT = 0.08

EXPERIMENTAL_CATEGORIES = {"MENTIONS"}


def load_csv(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    for row in rows:
        row["final_wss"] = float(row["final_wss"])
        row["leaderboard_pnl"] = float(row["leaderboard_pnl"])
        row["leaderboard_volume"] = float(row["leaderboard_volume"])

    return rows


def normalize_weights(rows: list[dict]) -> None:
    total_score = sum(row["final_wss"] for row in rows)
    if total_score <= 0:
        for row in rows:
            row["raw_weight"] = 0.0
        return

    for row in rows:
        row["raw_weight"] = row["final_wss"] / total_score


def apply_wallet_caps(rows: list[dict]) -> None:
    for row in rows:
        row["weight"] = min(row["raw_weight"], MAX_WALLET_WEIGHT)


def renormalize(rows: list[dict]) -> None:
    total_weight = sum(row["weight"] for row in rows)
    if total_weight <= 0:
        return
    for row in rows:
        row["weight"] /= total_weight


def apply_category_caps(rows: list[dict]) -> None:
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["category"]].append(row)

    for category, items in grouped.items():
        category_weight = sum(row["weight"] for row in items)
        category_cap = MAX_EXPERIMENTAL_WEIGHT if category in EXPERIMENTAL_CATEGORIES else MAX_CATEGORY_WEIGHT

        if category_weight <= category_cap:
            continue

        scale = category_cap / category_weight
        for row in items:
            row["weight"] *= scale


def final_rescale(rows: list[dict]) -> None:
    total_weight = sum(row["weight"] for row in rows)
    if total_weight <= 0:
        return
    for row in rows:
        row["weight"] /= total_weight


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


def print_summary(rows: list[dict]) -> None:
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
        print(f"{category}: {total:.4f}")


def main() -> None:
    rows = load_csv(INPUT_FILE)

    normalize_weights(rows)
    apply_wallet_caps(rows)
    renormalize(rows)
    apply_category_caps(rows)
    final_rescale(rows)

    rows.sort(key=lambda x: x["weight"], reverse=True)

    print_summary(rows)
    save_csv(rows, OUTPUT_FILE)
    print(f"\nSaved: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
