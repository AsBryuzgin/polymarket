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
from execution.state_backup import backup_state_db
from execution.state_store import init_db


def main() -> None:
    parser = argparse.ArgumentParser(description="Backup the configured executor SQLite state DB.")
    parser.add_argument("--label", default="manual", help="Short backup label.")
    args = parser.parse_args()

    init_db()
    config = load_executor_config()
    result = backup_state_db(config=config, label=args.label)

    print("=== STATE DB BACKUP ===")
    pprint(asdict(result))
    if not result.created:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
