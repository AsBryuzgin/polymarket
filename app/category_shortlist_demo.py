from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from collectors.leaderboard import LeaderboardClient
from collectors.wallet_profiles import WalletProfilesClient
from signals.wallet_metrics_builder import build_wallet_metrics
from signals.wallet_scoring import score_wallet


def _parse_ts(value: Any) -> datetime | None:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        try:
            v = float(value)
            if v > 10_000_000_000:
                v = v / 1000.0
            return datetime.fromtimestamp(v, tz=timezone.utc)
        except Exception:
            return None

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None

        if s.isdigit():
            try:
                v = float(s)
                if v > 10_000_000_000:
                    v = v / 1000.0
                return datetime.fromtimestamp(v, tz=timezone.utc)
            except Exception:
                pass

        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return None

    return None


def _extract_closed_position_ts(pos: dict[str, Any]) -> datetime | None:
    candidates = [
        pos.get("closedAt"),
        pos.get("closeTime"),
        pos.get("closeTimestamp"),
        pos.get("timestamp"),
        pos.get("updatedAt"),
        pos.get("createdAt"),
        pos.get("endDate"),
        pos.get("end_time"),
    ]

    for candidate in candidates:
        dt = _parse_ts(candidate)
        if dt is not None:
            return dt

    return None


def _position_dedupe_key(pos: dict[str, Any]) -> str:
    return str(
        pos.get("id")
        or pos.get("positionId")
        or pos.get("transactionHash")
        or pos.get("orderId")
        or f"{pos.get('asset')}|{pos.get('slug')}|{pos.get('outcome')}|{pos.get('size')}|{pos.get('price')}"
    )


def paginate_recent_closed_positions(
    wallet_client: WalletProfilesClient,
    wallet: str,
    page_size: int = 100,
    max_pages: int = 10,
) -> list[dict[str, Any]]:
    all_rows: list[dict[str, Any]] = []
    seen: set[str] = set()

    for page in range(max_pages):
        offset = page * page_size

        try:
            rows = wallet_client.get_closed_positions(
                user=wallet,
                limit=page_size,
                offset=offset,
                sort_by="CLOSETIME",
                sort_direction="DESC",
            )
        except Exception:
            try:
                rows = wallet_client.get_closed_positions(
                    user=wallet,
                    limit=page_size,
                    offset=offset,
                    sort_by="TIMESTAMP",
                    sort_direction="DESC",
                )
            except Exception:
                rows = wallet_client.get_closed_positions(
                    user=wallet,
                    limit=page_size,
                    offset=offset,
                )

        if not rows:
            break

        for row in rows:
            key = _position_dedupe_key(row)
            if key in seen:
                continue
            seen.add(key)
            all_rows.append(row)

        if len(rows) < page_size:
            break

    all_rows.sort(
        key=lambda x: _extract_closed_position_ts(x) or datetime(1970, 1, 1, tzinfo=timezone.utc),
        reverse=True,
    )
    return all_rows


def score_wallet_from_category_entry(
    wallet_client: WalletProfilesClient,
    entry: dict[str, Any],
) -> dict[str, Any]:
    wallet = entry["proxy_wallet"]

    profile = wallet_client.get_public_profile(wallet)
    traded_payload = wallet_client.get_total_markets_traded(wallet)
    traded_count = wallet_client.summarize_total_markets_traded(traded_payload)

    current_positions = wallet_client.paginate_current_positions(
        wallet,
        page_size=100,
        max_pages=3,
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
        "closed_positions_used": len(closed_positions),
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

    results: list[dict[str, Any]] = []

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
                    "closed_positions_used": 0,
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
            f"closed_used={item['closed_positions_used']:>4} | "
            f"pnl={round(item['leaderboard_pnl'], 2)} | "
            f"wallet={item['wallet']}"
        )
        if item["filter_reasons"]:
            print(f" reasons={item['filter_reasons']}")
        print()


if __name__ == "__main__":
    main()
