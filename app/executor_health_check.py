import sys
from pathlib import Path
from pprint import pprint

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.health_check import executor_health_report


def main() -> None:
    print("=== Executor Health Check ===")
    pprint(executor_health_report())


if __name__ == "__main__":
    main()
