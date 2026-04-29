from __future__ import annotations

import csv
import sys
from dataclasses import replace
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
from signals.copyability_history import record_copyability_score

OUTPUT_DIR = Path("data/shortlists")

CORE_CATEGORIES = [
    "SPORTS",
    "POLITICS",
    "FINANCE",
    "ECONOMICS",
    "CRYPTO",
    "TECH",
    "CULTURE",
    "WEATHER",
]

EXPERIMENTAL_CATEGORIES = [
    "MENTIONS",
]

TIME_PERIOD = "MONTH"
CANDIDATE_LIMIT = 30
WEEK_LOOKUP_LIMIT = 250


def score_wallet_from_category_entry(
    wallet_client: WalletProfilesClient,
    entry: dict,
    *,
    leaderboard_week_pnl: float | None = None,
) -> dict:
    wallet = entry["proxy_wallet"]

    profile = wallet_client.get_public_profile(wallet)
    traded_payload = wallet_client.get_total_markets_traded(wallet)
    traded_count = wallet_client.summarize_total_markets_traded(traded_payload)

    current_positions = wallet_client.paginate_current_positions(
        wallet,
        page_size=100,
        max_pages=20,
        sort_by="CURRENT",
    )
    closed_positions = paginate_recent_closed_positions(
        wallet_client=wallet_client,
        wallet=wallet,
        page_size=100,
        max_pages=10,
    )
    trades = wallet_client.paginate_trades(wallet, page_size=100, max_pages=3, taker_only=True)

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
        leaderboard_week_pnl=leaderboard_week_pnl,
        leaderboard_month_pnl=entry["pnl"],
    )

    raw_copyability_score = score_wallet(metrics).copyability_score
    smoothed_copyability_score, copyability_smoothing_samples = record_copyability_score(
        wallet=wallet,
        category=entry["leaderboard_category"],
        score=raw_copyability_score,
    )
    metrics = replace(metrics, copyability_score_override=smoothed_copyability_score)
    score = score_wallet(metrics)

    return {
        "rank": entry["rank"],
        "category": entry["leaderboard_category"],
        "time_period": entry["leaderboard_time_period"],
        "user_name": entry["user_name"],
        "wallet": wallet,
        "leaderboard_pnl": entry["pnl"],
        "leaderboard_week_pnl": leaderboard_week_pnl,
        "leaderboard_month_pnl": entry["pnl"],
        "leaderboard_volume": entry["volume"],
        "eligible": score.eligible,
        "final_wss": score.final_wss,
        "raw_wss": score.raw_wss,
        "consistency_score": score.consistency_score,
        "drawdown_score": score.drawdown_score,
        "specialization_score": score.specialization_score,
        "copyability_score": score.copyability_score,
        "copyability_score_raw": raw_copyability_score,
        "copyability_smoothing_samples": copyability_smoothing_samples,
        "activity_score": score.activity_score,
        "return_quality_score": score.return_quality_score,
        "track_record_multiplier": score.track_record_multiplier,
        "data_depth_multiplier": score.data_depth_multiplier,
        "filter_reasons": "; ".join(score.filter_reasons),
        "median_spread": median_spread,
        "median_liquidity": median_liquidity,
        "slippage_proxy": slippage_proxy,
        "current_position_pnl_ratio": metrics.current_position_pnl_ratio,
        "total_pnl_ratio": metrics.total_pnl_ratio,
        "open_loss_exposure": metrics.open_loss_exposure,
        "roi_7": metrics.roi_7,
        "roi_30": metrics.roi_30,
        "trades_30d": metrics.trades_30d,
        "trades_90d": metrics.trades_90d,
        "buy_trades_30d": metrics.buy_trades_30d,
        "sell_trades_30d": metrics.sell_trades_30d,
        "buy_trade_share_30d": metrics.buy_trade_share_30d,
        "days_since_last_trade": metrics.days_since_last_trade,
        "closed_positions_used": len(closed_positions),
    }


def run_category(
    leaderboard_client: LeaderboardClient,
    wallet_client: WalletProfilesClient,
    category: str,
) -> list[dict]:
    rows = leaderboard_client.get_leaderboard(
        category=category,
        time_period=TIME_PERIOD,
        order_by="PNL",
        limit=CANDIDATE_LIMIT,
        offset=0,
    )

    try:
        week_rows = leaderboard_client.get_leaderboard(
            category=category,
            time_period="WEEK",
            order_by="PNL",
            limit=WEEK_LOOKUP_LIMIT,
            offset=0,
        )
    except Exception:
        week_rows = []
    week_pnl_by_wallet = {
        str(row.get("proxyWallet") or "").lower(): float(row.get("pnl", 0) or 0)
        for row in week_rows
        if row.get("proxyWallet")
    }

    candidates = [
        leaderboard_client.normalize_entry(row, category=category, time_period=TIME_PERIOD)
        for row in rows
    ]

    results = []

    print(f"\nScoring category={category} top-{CANDIDATE_LIMIT}")

    for idx, entry in enumerate(candidates, start=1):
        wallet = entry["proxy_wallet"]
        user_name = entry["user_name"]

        print(f"[{idx}/{len(candidates)}] {category} | {user_name} | {wallet}")

        try:
            result = score_wallet_from_category_entry(
                wallet_client,
                entry,
                leaderboard_week_pnl=week_pnl_by_wallet.get(str(wallet).lower()),
            )
            results.append(result)
        except Exception as e:
            results.append(
                {
                    "rank": entry["rank"],
                    "category": category,
                    "time_period": TIME_PERIOD,
                    "user_name": user_name,
                    "wallet": wallet,
                    "leaderboard_pnl": entry["pnl"],
                    "leaderboard_week_pnl": week_pnl_by_wallet.get(str(wallet).lower()),
                    "leaderboard_month_pnl": entry["pnl"],
                    "leaderboard_volume": entry["volume"],
                    "eligible": False,
                    "final_wss": -1.0,
                    "raw_wss": -1.0,
                    "consistency_score": -1.0,
                    "drawdown_score": -1.0,
                    "specialization_score": -1.0,
                    "copyability_score": -1.0,
                    "copyability_score_raw": -1.0,
                    "copyability_smoothing_samples": 0,
                    "activity_score": -1.0,
                    "return_quality_score": -1.0,
                    "track_record_multiplier": -1.0,
                    "data_depth_multiplier": -1.0,
                    "filter_reasons": f"error: {e}",
                    "median_spread": None,
                    "median_liquidity": None,
                    "slippage_proxy": None,
                    "current_position_pnl_ratio": 0.0,
                    "total_pnl_ratio": 0.0,
                    "open_loss_exposure": 0.0,
                    "roi_7": 0.0,
                    "roi_30": 0.0,
                    "trades_30d": 0,
                    "trades_90d": 0,
                    "buy_trades_30d": 0,
                    "sell_trades_30d": 0,
                    "buy_trade_share_30d": 0.0,
                    "days_since_last_trade": 9999,
                    "closed_positions_used": 0,
                }
            )

    results.sort(
        key=lambda x: (int(bool(x["eligible"])), float(x["final_wss"])),
        reverse=True,
    )

    return results


def save_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        return

    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "category",
        "time_period",
        "rank",
        "user_name",
        "wallet",
        "leaderboard_pnl",
        "leaderboard_week_pnl",
        "leaderboard_month_pnl",
        "leaderboard_volume",
        "eligible",
        "final_wss",
        "raw_wss",
        "consistency_score",
        "drawdown_score",
        "specialization_score",
        "copyability_score",
        "copyability_score_raw",
        "copyability_smoothing_samples",
        "activity_score",
        "return_quality_score",
        "track_record_multiplier",
        "data_depth_multiplier",
        "filter_reasons",
        "median_spread",
        "median_liquidity",
        "slippage_proxy",
        "current_position_pnl_ratio",
        "total_pnl_ratio",
        "open_loss_exposure",
        "roi_7",
        "roi_30",
        "trades_30d",
        "trades_90d",
        "buy_trades_30d",
        "sell_trades_30d",
        "buy_trade_share_30d",
        "days_since_last_trade",
        "closed_positions_used",
    ]

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def print_top(rows: list[dict], top_n: int = 5) -> None:
    for row in rows[:top_n]:
        print(
            f"rank={row['rank']:>2} | "
            f"user={row['user_name']} | "
            f"wss={row['final_wss']:>6} | "
            f"eligible={row['eligible']} | "
            f"activity={row['activity_score']} | "
            f"trades30={row['trades_30d']} | "
            f"week_pnl={row.get('leaderboard_week_pnl')} | "
            f"month_pnl={round(float(row.get('leaderboard_month_pnl') or row['leaderboard_pnl']), 2)} | "
            f"buy30={row.get('buy_trades_30d', '')} | "
            f"sell30={row.get('sell_trades_30d', '')} | "
            f"spread={row['median_spread']} | "
            f"slip={row['slippage_proxy']} | "
            f"open_pnl={row['current_position_pnl_ratio']} | "
            f"total_pnl={row.get('total_pnl_ratio')} | "
            f"pnl={round(row['leaderboard_pnl'], 2)} | "
            f"wallet={row['wallet']}"
        )
        if row["filter_reasons"]:
            print(f" reasons={row['filter_reasons']}")


def run_group(
    categories: list[str],
    leaderboard_client: LeaderboardClient,
    wallet_client: WalletProfilesClient,
    group_name: str,
) -> list[dict]:
    group_rows: list[dict] = []

    for category in categories:
        rows = run_category(leaderboard_client, wallet_client, category)
        group_rows.extend(rows)

        print("\n" + "=" * 120)
        print(f"TOP RESULTS | GROUP={group_name} | CATEGORY={category}")
        print("=" * 120)
        print_top(rows, top_n=5)

        category_file = OUTPUT_DIR / f"{category.lower()}_shortlist.csv"
        save_csv(rows, category_file)
        print(f"\nSaved: {category_file}")

    return group_rows


def main() -> None:
    leaderboard_client = LeaderboardClient()
    wallet_client = WalletProfilesClient()

    core_rows = run_group(
        categories=CORE_CATEGORIES,
        leaderboard_client=leaderboard_client,
        wallet_client=wallet_client,
        group_name="core",
    )

    experimental_rows = run_group(
        categories=EXPERIMENTAL_CATEGORIES,
        leaderboard_client=leaderboard_client,
        wallet_client=wallet_client,
        group_name="experimental",
    )

    save_csv(core_rows, OUTPUT_DIR / "master_shortlist_core.csv")
    print(f"\nSaved core shortlist: {OUTPUT_DIR / 'master_shortlist_core.csv'}")

    save_csv(experimental_rows, OUTPUT_DIR / "master_shortlist_experimental.csv")
    print(f"Saved experimental shortlist: {OUTPUT_DIR / 'master_shortlist_experimental.csv'}")


if __name__ == "__main__":
    main()
