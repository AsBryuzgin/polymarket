import sys
from pathlib import Path
from pprint import pprint

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.state_store import init_db, list_trade_history

def main() -> None:
    init_db()
    pprint(list_trade_history(limit=50))

if __name__ == "__main__":
    main()
