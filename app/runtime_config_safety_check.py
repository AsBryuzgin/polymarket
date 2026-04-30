from __future__ import annotations

import sys
from pathlib import Path
from pprint import pprint

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.builder_auth import load_executor_config
from execution.config_safety import build_config_safety_report


def main() -> None:
    config = load_executor_config()
    report = build_config_safety_report(config)
    print("=== RUNTIME CONFIG SAFETY CHECK ===")
    pprint(report)
    if report["status"] == "NO_GO":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
