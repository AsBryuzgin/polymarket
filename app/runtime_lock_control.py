from __future__ import annotations

import argparse
import sys
from dataclasses import asdict
from pathlib import Path
from pprint import pprint

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.builder_auth import load_executor_config
from execution.runtime_lock import activate_runtime_lock, clear_runtime_lock, read_runtime_lock


def main() -> None:
    parser = argparse.ArgumentParser(description="View, activate, or clear the runtime trading lock.")
    parser.add_argument("--activate", action="store_true", help="Activate the runtime lock.")
    parser.add_argument("--clear", action="store_true", help="Clear the runtime lock.")
    parser.add_argument("--reason", default="manual runtime lock", help="Reason for --activate.")
    args = parser.parse_args()

    config = load_executor_config()

    if args.activate and args.clear:
        raise SystemExit("Use only one of --activate or --clear.")
    if args.activate:
        state = activate_runtime_lock(
            config,
            reason=args.reason,
            source="runtime_lock_control",
        )
    elif args.clear:
        state = clear_runtime_lock(config)
    else:
        state = read_runtime_lock(config)

    print("=== RUNTIME LOCK ===")
    pprint(asdict(state))


if __name__ == "__main__":
    main()
