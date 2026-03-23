from collectors.leaderboard import LeaderboardClient


def print_category(category: str, time_period: str = "MONTH", limit: int = 5) -> None:
    client = LeaderboardClient()
    rows = client.get_leaderboard(
        category=category,
        time_period=time_period,
        order_by="PNL",
        limit=limit,
        offset=0,
    )

    normalized = [
        client.normalize_entry(row, category=category, time_period=time_period)
        for row in rows
    ]

    print("=" * 90)
    print(f"CATEGORY: {category} | PERIOD: {time_period}")
    print("=" * 90)

    for row in normalized:
        print(
            f"rank={row['rank']} | user={row['user_name']} | "
            f"wallet={row['proxy_wallet']} | pnl={row['pnl']} | vol={row['volume']}"
        )
    print()


def main() -> None:
    for category in ["SPORTS", "POLITICS", "FINANCE", "WEATHER"]:
        print_category(category=category, time_period="MONTH", limit=5)


if __name__ == "__main__":
    main()
