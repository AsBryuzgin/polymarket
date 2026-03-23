from __future__ import annotations

from collectors.leaderboard import LeaderboardClient
from collectors.wallet_profiles import WalletProfilesClient


def main() -> None:
    leaderboard_client = LeaderboardClient()
    wallet_client = WalletProfilesClient()

    leaderboard = leaderboard_client.get_leaderboard()
    top_entries = [leaderboard_client.normalize_entry(x) for x in leaderboard[:5]]

    print(f"Loaded {len(top_entries)} leaderboard candidates\n")

    for entry in top_entries:
        wallet = entry["proxy_wallet"]
        if not wallet:
            continue

        print("=" * 80)
        print(f"rank: {entry['rank']}")
        print(f"wallet: {wallet}")
        print(f"user_name: {entry['user_name']}")
        print(f"leaderboard_pnl: {entry['pnl']}")
        print(f"leaderboard_volume: {entry['volume']}")
        print(f"x_username: {entry['x_username']}")
        print(f"verified_badge: {entry['verified_badge']}")

        try:
            profile = wallet_client.get_public_profile(wallet)
            profile_summary = wallet_client.summarize_profile(profile)
            print(f"profile_name: {profile_summary['name']}")
            print(f"profile_pseudonym: {profile_summary['pseudonym']}")
            print(f"profile_created_at: {profile_summary['created_at']}")
        except Exception as e:
            print(f"profile_error: {e}")

        try:
            traded_payload = wallet_client.get_total_markets_traded(wallet)
            traded_count = wallet_client.summarize_total_markets_traded(traded_payload)
            print(f"markets_traded: {traded_count}")
        except Exception as e:
            print(f"traded_error: {e}")

        try:
            current_positions = wallet_client.get_current_positions(wallet, limit=100)
            current_summary = wallet_client.summarize_positions(current_positions)
            print(f"open_positions_count: {current_summary['open_positions_count']}")
            print(f"open_current_value: {current_summary['open_current_value']}")
            print(f"open_cash_pnl: {current_summary['open_cash_pnl']}")
        except Exception as e:
            print(f"current_positions_error: {e}")

        try:
            closed_positions = wallet_client.get_closed_positions(wallet, limit=50)
            closed_summary = wallet_client.summarize_closed_positions(closed_positions)
            print(f"closed_positions_count_sample: {closed_summary['closed_positions_count']}")
            print(f"closed_realized_pnl_sum_sample: {closed_summary['closed_realized_pnl_sum']}")
        except Exception as e:
            print(f"closed_positions_error: {e}")

        try:
            trades = wallet_client.get_trades(wallet, limit=100)
            trade_summary = wallet_client.summarize_trades(trades)
            print(f"trade_count_sample: {trade_summary['trade_count']}")
            print(f"buy_count_sample: {trade_summary['buy_count']}")
            print(f"sell_count_sample: {trade_summary['sell_count']}")
            print(f"trade_notional_sum_sample: {trade_summary['trade_notional_sum']}")
        except Exception as e:
            print(f"trades_error: {e}")

        print()


if __name__ == "__main__":
    main()
