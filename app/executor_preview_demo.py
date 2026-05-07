import sys
from pathlib import Path
from pprint import pprint

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.polymarket_executor import preview_market_order


def main() -> None:
    print("=== Executor Preview Demo ===")
    result = preview_market_order()
    pprint(result)


if __name__ == "__main__":
    main()
