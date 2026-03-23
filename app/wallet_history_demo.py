from __future__ import annotations

from collectors.wallet_profiles import WalletProfilesClient


def main() -> None:
    wallet = "0xdb27bf2ac5d428a9c63dbc914611036855a6c56e"  # DrPufferfish
    client = WalletProfilesClient()

    print(f"Loading full-ish history for wallet: {wallet}\n")

    profile = client.get_public_profile(wallet)
    profile_summary = client.summarize_profile(profile)
    print("PROFILE")
    print(profile_summary)
    print()

    traded_payload = client.get_total_markets_traded(wallet)
    traded_count = client.summarize_total_markets_traded(traded_payload)
    print("MARKETS TRADED")
    print({"markets_traded": traded_count})
    print()

    current_positions = client.paginate_current_positions(wallet, page_size=100, max_pages=10)
    current_summary = client.summarize_positions(current_positions)
    print("CURRENT POSITIONS")
    print(current_summary)
    print()

    closed_positions = client.paginate_closed_positions(wallet, page_size=100, max_pages=10)
    closed_summary = client.summarize_closed_positions(closed_positions)
    print("CLOSED POSITIONS")
    print(closed_summary)
    print()

    trades = client.paginate_trades(wallet, page_size=100, max_pages=10, taker_only=True)
    trades_summary = client.summarize_trades(trades)
    print("TRADES")
    print(trades_summary)
    print()


if __name__ == "__main__":
    main()
