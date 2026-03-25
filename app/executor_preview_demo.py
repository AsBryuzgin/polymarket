from pprint import pprint

from execution.polymarket_executor import preview_market_order


def main() -> None:
    print("=== Executor Preview Demo ===")
    result = preview_market_order()
    pprint(result)


if __name__ == "__main__":
    main()
