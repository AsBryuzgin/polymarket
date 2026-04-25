from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from collectors.leaderboard import LeaderboardClient
from collectors.wallet_profiles import WalletProfilesClient
from signals.wallet_metrics_builder import build_wallet_metrics
from signals.wallet_scoring import score_wallet
from signals.shortlist_helpers import (
    paginate_recent_closed_positions,
    estimate_copyability_inputs,
)


def score_wallet_from_category_entry(
    wallet_client: WalletProfilesClient,
    entry: dict[str, object],
) -> dict[str, object]:
    wallet = str(entry["proxy_wallet"])

    profile = wallet_client.get_public_profile(wallet)
    traded_payload = wallet_client.get_total_markets_traded(wallet)
    traded_count = wallet_client.summarize_total_markets_traded(traded_payload)

    current_positions = wallet_client.paginate_current_positions(
        wallet,
        page_size=100,
        max_pages=20,
        sort_by="CURRENT",
    )
    trades = wallet_client.paginate_trades(
        wallet,
        page_size=100,
        max_pages=3,
        taker_only=True,
    )
    closed_positions = paginate_recent_closed_positions(
        wallet_client=wallet_client,
        wallet=wallet,
        page_size=100,
        max_pages=10,
    )

    median_spread, median_liquidity, slippage_proxy, delay_sec = estimate_copyability_inputs(
        current_positions=current_positions,
        trades=trades,
    )

    metrics = build_wallet_metrics(
        profile=profile,
        traded_count=traded_count,
        current_positions=current_positions,
        closed_positions=closed_positions,
        trades=trades,
        median_spread=median_spread,
        median_liquidity=median_liquidity,
        slippage_proxy=slippage_proxy,
        delay_sec=delay_sec,
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
        "consistency_score": score.consistency_score,
        "drawdown_score": score.drawdown_score,
        "specialization_score": score.specialization_score,
        "copyability_score": score.copyability_score,
        "activity_score": score.activity_score,
        "return_quality_score": score.return_quality_score,
        "track_record_multiplier": score.track_record_multiplier,
        "data_depth_multiplier": score.data_depth_multiplier,
        "filter_reasons": score.filter_reasons,
        "closed_positions_used": len(closed_positions),
        "trades_30d": metrics.trades_30d,
        "trades_90d": metrics.trades_90d,
        "buy_trades_30d": metrics.buy_trades_30d,
        "sell_trades_30d": metrics.sell_trades_30d,
        "buy_trade_share_30d": metrics.buy_trade_share_30d,
        "days_since_last_trade": metrics.days_since_last_trade,
        "median_spread": median_spread,
        "median_liquidity": median_liquidity,
        "slippage_proxy": slippage_proxy,
        "current_position_pnl_ratio": metrics.current_position_pnl_ratio,
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

    results: list[dict[str, object]] = []

    print(f"Scoring top-{candidate_limit} wallets for category={category}, period={time_period}\n")

    for idx, entry in enumerate(candidates, start=1):
        wallet = str(entry["proxy_wallet"])
        user_name = str(entry["user_name"])

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
                    "activity_score": -1.0,
                    "filter_reasons": [f"error: {e}"],
                    "closed_positions_used": 0,
                    "trades_30d": 0,
                    "trades_90d": 0,
                    "buy_trades_30d": 0,
                    "sell_trades_30d": 0,
                    "buy_trade_share_30d": 0.0,
                    "days_since_last_trade": 9999,
                    "median_spread": None,
                    "median_liquidity": None,
                    "slippage_proxy": None,
                    "current_position_pnl_ratio": 0.0,
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
            f"activity={item['activity_score']} | "
            f"trades30={item['trades_30d']} | "
            f"buy30={item.get('buy_trades_30d', '')} | "
            f"sell30={item.get('sell_trades_30d', '')} | "
            f"closed_used={item['closed_positions_used']:>4} | "
            f"spread={item['median_spread']} | "
            f"slip={item['slippage_proxy']} | "
            f"open_pnl={item['current_position_pnl_ratio']} | "
            f"pnl={round(float(item['leaderboard_pnl']), 2)} | "
            f"wallet={item['wallet']}"
        )
        if item["filter_reasons"]:
            print(f" reasons={item['filter_reasons']}")
        print()


if __name__ == "__main__":
    main()
