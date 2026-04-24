from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from pprint import pprint

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.builder_auth import load_executor_config
from execution.onchain_shadow import init_onchain_shadow_tables, poll_onchain_shadow_once
from execution.state_store import init_db


def main() -> None:
    parser = argparse.ArgumentParser(description="Run on-chain leader-fill shadow polling.")
    parser.add_argument("--interval-sec", type=float, default=None)
    parser.add_argument("--max-cycles", type=int, default=0)
    args = parser.parse_args()

    config = load_executor_config()
    cfg = config.get("onchain_shadow", {})
    if not bool(cfg.get("enabled", False)):
        raise SystemExit("onchain_shadow.enabled is false")

    interval_sec = float(args.interval_sec or cfg.get("poll_interval_sec", 4.0))
    init_db()
    init_onchain_shadow_tables()

    print("=== ONCHAIN SHADOW LOOP ===")
    pprint({"interval_sec": interval_sec, "max_cycles": args.max_cycles or "infinite"})

    cycle = 0
    while True:
        cycle += 1
        started = time.monotonic()
        try:
            summary = poll_onchain_shadow_once(config)
        except Exception as e:
            summary = {"status": "ERROR", "error": str(e)}
        print(f"cycle {cycle}:")
        pprint(summary)

        if args.max_cycles and cycle >= args.max_cycles:
            return

        elapsed = time.monotonic() - started
        time.sleep(max(0.0, interval_sec - elapsed))


if __name__ == "__main__":
    main()

