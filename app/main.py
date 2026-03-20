from collectors.gamma_markets import GammaMarketsClient


def main() -> None:
    client = GammaMarketsClient()
    markets = client.get_markets(limit=5, active=True, closed=False)

    print(f"Loaded {len(markets)} markets\n")

    for idx, market in enumerate(markets, start=1):
        item = client.normalize_market(market)
        print(f"{idx}. {item['question']}")
        print(f"   id: {item['id']}")
        print(f"   active: {item['active']} | closed: {item['closed']}")
        print(f"   liquidity: {item['liquidity']} | volume: {item['volume']}")
        print(f"   end_date: {item['end_date']}")
        print()


if __name__ == "__main__":
    main()
