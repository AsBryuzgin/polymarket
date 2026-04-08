from pprint import pprint
from execution.state_store import init_db, list_trade_history

def main() -> None:
    init_db()
    pprint(list_trade_history(limit=50))

if __name__ == "__main__":
    main()
