from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path

INPUT_FILE = Path("data/shortlists/final_portfolio_allocation.csv")
OUTPUT_FILE = Path("data/shortlists/live_portfolio_allocation.csv")

EXCLUDE_CATEGORIES = {"MENTIONS"}


def load_csv(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    for row in rows:
        row["weight"] = float(row["weight"])
        row["final_wss"] = float(row["final_wss"])
        row["leaderboard_pnl"] = float(row["leaderboard_pnl"])
        row["leaderboard_volume"] = float(row["leaderboard_volume"])

    return rows


def save_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        return

    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    rows = load_csv(INPUT_FILE)

    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        if row["category"] in EXCLUDE_CATEGORIES:
            continue
        grouped[row["category"]].append(row)

    selected = []
    for category, items in grouped.items():
        items.sort(key=lambda x: x["final_wss"], reverse=True)
        selected.append(items[0])

    # Перенормируем веса только по live universe
    total_weight = sum(row["weight"] for row in selected)
    for row in selected:
        row["weight"] = round(row["weight"] / total_weight, 6) if total_weight > 0 else 0.0

    selected.sort(key=lambda x: x["weight"], reverse=True)

    print("=== LIVE UNIVERSE ===")
    for row in selected:
        print(
            f"{row['category']}: {row['user_name']} | "
            f"wss={row['final_wss']:.2f} | weight={row['weight']:.4f} | wallet={row['wallet']}"
        )

    save_csv(selected, OUTPUT_FILE)
    print(f"\nSaved: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
