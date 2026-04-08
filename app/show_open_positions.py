from pprint import pprint
from execution.state_store import init_db, list_open_positions

def main() -> None:
    init_db()
    pprint(list_open_positions(limit=100))

if __name__ == "__main__":
    main()
