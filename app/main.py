from collectors.gamma_markets import GammaMarketsClient
from collectors.clob_prices import ClobPricesClient


def main() -> None:
    gamma = GammaMarketsClient()
    clob = ClobPricesClient()

    markets = gamma.get_markets(limit=3, active=True, closed=False)
    print(f"Loaded {len(markets)} markets\n")

    for idx, market in enumerate(markets, start=1):
        item = gamma.normalize_market(market)
        yes_token_id = item["yes_token_id"]

        midpoint = None
        spread = None
        midpoint_raw = None
        spread_raw = None

        if yes_token_id:
            try:
                midpoint_raw = clob.get_midpoint_raw(yes_token_id)
                midpoint = clob.get_midpoint(yes_token_id)
            except Exception as e:
                midpoint_raw = f"error: {e}"
                midpoint = f"error: {e}"

            try:
                spread_raw = clob.get_spread_raw(yes_token_id)
                spread = clob.get_spread(yes_token_id)
            except Exception as e:
                spread_raw = f"error: {e}"
                spread = f"error: {e}"

        print(f"{idx}. {item['question']}")
        print(f"   market_id: {item['id']}")
        print(f"   yes_token_id: {item['yes_token_id']}")
        print(f"   no_token_id:  {item['no_token_id']}")
        print(f"   midpoint_raw: {midpoint_raw}")
        print(f"   spread_raw:   {spread_raw}")
        print(f"   yes_midpoint: {midpoint}")
        print(f"   yes_spread:   {spread}")
        print()


if __name__ == "__main__":
    main()
