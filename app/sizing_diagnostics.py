from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.builder_auth import load_executor_config
from execution.telegram_reports import build_sizing_report


def main() -> None:
    print(build_sizing_report(load_executor_config()))


if __name__ == "__main__":
    main()
