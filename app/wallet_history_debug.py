from collectors.wallet_profiles import WalletProfilesClient
import json


def main() -> None:
    wallet = "0xdb27bf2ac5d428a9c63dbc914611036855a6c56e"
    client = WalletProfilesClient()

    current_positions = client.paginate_current_positions(wallet, page_size=5, max_pages=1)
    closed_positions = client.paginate_closed_positions(wallet, page_size=5, max_pages=1)
    trades = client.paginate_trades(wallet, page_size=5, max_pages=1, taker_only=True)

    if current_positions:
        print("CURRENT POSITION KEYS:")
        print(list(current_positions[0].keys()))
        print(json.dumps(current_positions[0], indent=2, ensure_ascii=False))
        print()

    if closed_positions:
        print("CLOSED POSITION KEYS:")
        print(list(closed_positions[0].keys()))
        print(json.dumps(closed_positions[0], indent=2, ensure_ascii=False))
        print()

    if trades:
        print("TRADE KEYS:")
        print(list(trades[0].keys()))
        print(json.dumps(trades[0], indent=2, ensure_ascii=False))
        print()


if __name__ == "__main__":
    main()
