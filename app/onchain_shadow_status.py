from __future__ import annotations

import argparse
import sys
from pathlib import Path
from pprint import pprint

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.onchain_shadow import onchain_shadow_summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Show on-chain shadow latency summary.")
    parser.add_argument("--hours", type=int, default=24)
    args = parser.parse_args()
    print("=== ONCHAIN SHADOW STATUS ===")
    pprint(onchain_shadow_summary(hours=args.hours))


if __name__ == "__main__":
    main()

