from __future__ import annotations

import csv
from pathlib import Path


OLD_FILE = Path("data/shortlists/final_portfolio_allocation_previous.csv")
NEW_FILE = Path("data/shortlists/final_portfolio_allocation.csv")
OUTPUT_FILE = Path("data/shortlists/final_portfolio_rebalance.csv")

MIN_TRADE_DELTA = 0.01  # 1 percentage point


def load_csv(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    for row in rows:
        row["weight"] = float(row["weight"])
        row["final_wss"] = float(row["final_wss"])
        row["leaderboard_pnl"] = float(row["leaderboard_pnl"])
        row["leaderboard_volume"] = float(row["leaderboard_volume"])

    return rows


def index_by_wallet(rows: list[dict]) -> dict[str, dict]:
    return {row["wallet"]: row for row in rows}


def build_rebalance(old_rows: list[dict], new_rows: list[dict]) -> list[dict]:
    old_map = index_by_wallet(old_rows)
    new_map = index_by_wallet(new_rows)

    all_wallets = sorted(set(old_map.keys()) | set(new_map.keys()))
    result = []

    for wallet in all_wallets:
        old = old_map.get(wallet)
        new = new_map.get(wallet)

        old_weight = old["weight"] if old else 0.0
        new_weight = new["weight"] if new else 0.0
        delta = new_weight - old_weight

        if abs(delta) < MIN_TRADE_DELTA:
            action = "HOLD"
        elif delta > 0:
            action = "BUY"
        else:
            action = "SELL"

        source = new if new else old

        result.append(
            {
                "wallet": wallet,
                "user_name": source["user_name"],
                "category": source["category"],
                "old_weight": round(old_weight, 6),
                "new_weight": round(new_weight, 6),
                "delta": round(delta, 6),
                "action": action,
                "final_wss": round(source["final_wss"], 2),
            }
        )

    result.sort(key=lambda x: abs(x["delta"]), reverse=True)
    return result


def save_csv(rows: list[dict], path: Path) -> None:
    fieldnames = [
        "wallet",
        "user_name",
        "category",
        "old_weight",
        "new_weight",
        "delta",
        "action",
        "final_wss",
    ]

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def print_summary(rows: list[dict]) -> None:
    print("=" * 120)
    print("PORTFOLIO REBALANCE PLAN")
    print("=" * 120)

    for row in rows:
        print(
            f"user={row['user_name']} | "
            f"category={row['category']} | "
            f"old={row['old_weight']:.4f} | "
            f"new={row['new_weight']:.4f} | "
            f"delta={row['delta']:+.4f} | "
            f"action={row['action']} | "
            f"wallet={row['wallet']}"
        )


def main() -> None:
    if not OLD_FILE.exists():
        print(f"Previous allocation file not found: {OLD_FILE}")
        print("Create it first by copying the current allocation as a baseline.")
        return

    old_rows = load_csv(OLD_FILE)
    new_rows = load_csv(NEW_FILE)

    rebalance_rows = build_rebalance(old_rows, new_rows)
    print_summary(rebalance_rows)
    save_csv(rebalance_rows, OUTPUT_FILE)

    print(f"\nSaved: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
