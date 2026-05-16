from __future__ import annotations

import csv
import sys
from pathlib import Path
from pprint import pprint

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.signal_observation_store import init_signal_observation_table, list_signal_observations


OUT_FILE = Path("data/signal_observations.csv")


def main() -> None:
    init_signal_observation_table()
    rows = list_signal_observations(limit=100000)

    if not rows:
        print("No signal observations yet.")
        return

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with OUT_FILE.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    pprint(rows[:10])
    print(f"Saved: {OUT_FILE}")


if __name__ == "__main__":
    main()
