from __future__ import annotations

import csv
from pathlib import Path

from execution.state_store import init_db, list_trade_history


OUTPUT_FILE = Path("data/trade_history.csv")


def main() -> None:
    init_db()
    rows = list_trade_history(limit=100000)

    if not rows:
        print("No trade history rows yet.")
        return

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = list(rows[0].keys())
    with OUTPUT_FILE.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(reversed(rows))

    print(f"Saved: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
