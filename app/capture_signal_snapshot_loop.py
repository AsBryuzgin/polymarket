from __future__ import annotations

import sys
import time
from datetime import datetime, timezone

from app.capture_signal_snapshot import capture_once


def main() -> None:
    interval_sec = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    max_cycles = int(sys.argv[2]) if len(sys.argv) > 2 else 0

    cycle = 0
    print(f"Starting signal snapshot loop | interval_sec={interval_sec} | max_cycles={max_cycles or 'infinite'}")

    try:
        while True:
            cycle += 1
            started_at = datetime.now(timezone.utc).isoformat()
            print(f"\n--- signal snapshot cycle {cycle} started at {started_at} ---")
            rows = capture_once(verbose=True)
            print(f"cycle={cycle} | observed_rows={len(rows)}")

            if max_cycles and cycle >= max_cycles:
                print("Reached max_cycles, stopping.")
                break

            time.sleep(interval_sec)
    except KeyboardInterrupt:
        print("\nStopped by user.")


if __name__ == "__main__":
    main()
