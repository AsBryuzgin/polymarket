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
        best_bid = None
        best_ask = None
        book_raw = None

        if yes_token_id:
            try:
                midpoint = clob.get_midpoint(yes_token_id)
            except Exception as e:
                midpoint = f"error: {e}"

            try:
                spread = clob.get_spread(yes_token_id)
            except Exception as e:
                spread = f"error: {e}"

            try:
                book_raw = clob.get_book_raw(yes_token_id)
                best_bid, best_ask = clob.get_best_bid_ask(yes_token_id)
            except Exception as e:
                book_raw = f"error: {e}"
                best_bid = f"error: {e}"
                best_ask = f"error: {e}"

        print(f"{idx}. {item['question']}")
        print(f"   market_id: {item['id']}")
        print(f"   yes_token_id: {item['yes_token_id']}")
        print(f"   no_token_id:  {item['no_token_id']}")
        print(f"   yes_midpoint: {midpoint}")
        print(f"   yes_spread:   {spread}")
        print(f"   best_bid:     {best_bid}")
        print(f"   best_ask:     {best_ask}")
        print(f"   book_raw:     {book_raw}")
        print()


if __name__ == "__main__":
    main()
