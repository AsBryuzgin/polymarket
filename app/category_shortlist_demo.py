from __future__ import annotations

from collectors.leaderboard import LeaderboardClient
from collectors.wallet_profiles import WalletProfilesClient
from signals.wallet_metrics_builder import build_wallet_metrics
from signals.wallet_scoring import score_wallet


def score_wallet_from_category_entry(
    wallet_client: WalletProfilesClient,
    entry: dict,
) -> dict:
    wallet = entry["proxy_wallet"]

    profile = wallet_client.get_public_profile(wallet)
    traded_payload = wallet_client.get_total_markets_traded(wallet)
    traded_count = wallet_client.summarize_total_markets_traded(traded_payload)

    # v1: ограничиваем глубину истории, чтобы demo не был слишком медленным
    current_positions = wallet_client.paginate_current_positions(wallet, page_size=100, max_pages=3)
    closed_positions = wallet_client.paginate_closed_positions(wallet, page_size=100, max_pages=3)
    trades = wallet_client.paginate_trades(wallet, page_size=100, max_pages=3, taker_only=True)

    metrics = build_wallet_metrics(
        profile=profile,
        traded_count=traded_count,
        current_positions=current_positions,
        closed_positions=closed_positions,
        trades=trades,
        median_spread=0.015,
        median_liquidity=10000.0,
        slippage_proxy=0.01,
        delay_sec=60.0,
    )

    score = score_wallet(metrics)

    return {
        "rank": entry["rank"],
        "user_name": entry["user_name"],
        "wallet": wallet,
        "leaderboard_pnl": entry["pnl"],
        "leaderboard_volume": entry["volume"],
        "eligible": score.eligible,
        "final_wss": score.final_wss,
        "raw_wss": score.raw_wss,
        "filter_reasons": score.filter_reasons,
    }


def main() -> None:
    category = "SPORTS"
    time_period = "MONTH"
    candidate_limit = 15

    leaderboard_client = LeaderboardClient()
    wallet_client = WalletProfilesClient()

    rows = leaderboard_client.get_leaderboard(
        category=category,
        time_period=time_period,
        order_by="PNL",
        limit=candidate_limit,
        offset=0,
    )

    candidates = [
        leaderboard_client.normalize_entry(row, category=category, time_period=time_period)
        for row in rows
    ]

    results = []

    print(f"Scoring top-{candidate_limit} wallets for category={category}, period={time_period}\n")

    for idx, entry in enumerate(candidates, start=1):
        wallet = entry["proxy_wallet"]
        user_name = entry["user_name"]

        print(f"[{idx}/{len(candidates)}] scoring {user_name} | {wallet}")

        try:
            result = score_wallet_from_category_entry(wallet_client, entry)
            results.append(result)
        except Exception as e:
            results.append(
                {
                    "rank": entry["rank"],
                    "user_name": user_name,
                    "wallet": wallet,
                    "leaderboard_pnl": entry["pnl"],
                    "leaderboard_volume": entry["volume"],
                    "eligible": False,
                    "final_wss": -1.0,
                    "raw_wss": -1.0,
                    "filter_reasons": [f"error: {e}"],
                }
            )

    results.sort(
        key=lambda x: (
            int(bool(x["eligible"])),
            float(x["final_wss"]),
        ),
        reverse=True,
    )

    print("\n" + "=" * 120)
    print(f"SHORTLIST | CATEGORY={category} | PERIOD={time_period}")
    print("=" * 120)

    for item in results:
        print(
            f"rank={item['rank']:>2} | "
            f"user={item['user_name']} | "
            f"wss={item['final_wss']:>6} | "
            f"eligible={item['eligible']} | "
            f"pnl={round(item['leaderboard_pnl'], 2)} | "
            f"wallet={item['wallet']}"
        )
        if item["filter_reasons"]:
            print(f"   reasons={item['filter_reasons']}")
    print()


if __name__ == "__main__":
    main()
