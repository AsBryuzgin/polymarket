from collectors.gamma_markets import GammaMarketsClient
from collectors.clob_prices import ClobPricesClient


def main() -> None:
    gamma = GammaMarketsClient()
    clob = ClobPricesClient()

    markets = gamma.get_markets(limit=5, active=True, closed=False)
    print(f"Loaded {len(markets)} markets\n")

    for idx, market in enumerate(markets, start=1):
        item = gamma.normalize_market(market)

        yes_token_id = item["yes_token_id"]
        midpoint = None
        spread = None

        if yes_token_id:
            try:
                midpoint = clob.get_midpoint(yes_token_id)
            except Exception as e:
                midpoint = f"error: {e}"

            try:
                spread = clob.get_spread(yes_token_id)
            except Exception as e:
                spread = f"error: {e}"

        print(f"{idx}. {item['question']}")
        print(f"   market_id: {item['id']}")
        print(f"   yes_token_id: {item['yes_token_id']}")
        print(f"   no_token_id:  {item['no_token_id']}")
        print(f"   active: {item['active']} | closed: {item['closed']}")
        print(f"   liquidity: {item['liquidity']} | volume: {item['volume']}")
        print(f"   end_date: {item['end_date']}")
        print(f"   yes_midpoint: {midpoint}")
        print(f"   yes_spread:   {spread}")
        print()


if __name__ == "__main__":
    main()
