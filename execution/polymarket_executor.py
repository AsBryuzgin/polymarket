from __future__ import annotations

import os
from dataclasses import dataclass
from dotenv import load_dotenv

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import MarketOrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL

from execution.builder_auth import load_executor_env

load_dotenv()


@dataclass
class PreviewOrderConfig:
    token_id: str
    amount_usd: float
    side: str


def load_preview_config() -> PreviewOrderConfig:
    return PreviewOrderConfig(
        token_id=os.getenv("PREVIEW_TOKEN_ID", "").strip(),
        amount_usd=float(os.getenv("PREVIEW_AMOUNT_USD", "2")),
        side=os.getenv("PREVIEW_SIDE", "BUY").strip().upper(),
    )


def _side_constant(side: str):
    if side == "BUY":
        return BUY
    if side == "SELL":
        return SELL
    raise ValueError(f"Unsupported side: {side}")


def _market_order_amount(*, amount_usd: float, side: str, price_quote: float | None) -> float:
    side = side.upper()
    if side == "BUY":
        return amount_usd
    if side == "SELL":
        if price_quote is None or price_quote <= 0:
            raise ValueError("cannot convert SELL amount_usd to shares without a positive price quote")
        return amount_usd / price_quote
    raise ValueError(f"Unsupported side: {side}")


def _extract_best_bid_ask(book) -> tuple[float | None, float | None]:
    bid_prices = []
    ask_prices = []

    for level in getattr(book, "bids", []) or []:
        try:
            bid_prices.append(float(level.price))
        except Exception:
            pass

    for level in getattr(book, "asks", []) or []:
        try:
            ask_prices.append(float(level.price))
        except Exception:
            pass

    best_bid = max(bid_prices) if bid_prices else None
    best_ask = min(ask_prices) if ask_prices else None

    return best_bid, best_ask


def _extract_field(book, key: str):
    if isinstance(book, dict):
        return book.get(key)
    return getattr(book, key, None)


def _extract_float_field(book, key: str) -> float | None:
    raw = _extract_field(book, key)
    try:
        return float(raw) if raw is not None else None
    except Exception:
        return None


def build_authenticated_client() -> ClobClient:
    env = load_executor_env()
    client = ClobClient(
        env.clob_host,
        key=env.private_key,
        chain_id=env.chain_id,
        signature_type=env.signature_type,
        funder=env.funder_address,
    )
    client.set_api_creds(client.create_or_derive_api_creds())
    return client


def fetch_market_snapshot(token_id: str, side: str = "BUY") -> dict:
    client = build_authenticated_client()

    mid = client.get_midpoint(token_id)
    price_quote = client.get_price(token_id, side=side)
    book = client.get_order_book(token_id)
    best_bid, best_ask = _extract_best_bid_ask(book)
    min_order_size = _extract_float_field(book, "min_order_size")
    tick_size = _extract_float_field(book, "tick_size")
    neg_risk = _extract_field(book, "neg_risk")
    last_trade_price = _extract_float_field(book, "last_trade_price")

    mid_value = None
    if isinstance(mid, dict):
        raw = mid.get("mid")
        mid_value = float(raw) if raw is not None else None

    quote_value = None
    if isinstance(price_quote, dict):
        raw = price_quote.get("price")
        quote_value = float(raw) if raw is not None else None

    spread_value = None
    if best_bid is not None and best_ask is not None:
        spread_value = best_ask - best_bid

    return {
        "token_id": token_id,
        "side": side,
        "midpoint": mid_value,
        "price_quote": quote_value,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread_value,
        "min_order_size": min_order_size,
        "tick_size": tick_size,
        "neg_risk": neg_risk,
        "last_trade_price": last_trade_price,
        "raw_midpoint": mid,
        "raw_price_quote": price_quote,
    }


def preview_market_order(
    token_id: str,
    amount_usd: float,
    side: str = "BUY",
) -> dict:
    client = build_authenticated_client()
    snapshot = fetch_market_snapshot(token_id=token_id, side=side)
    order_amount = _market_order_amount(
        amount_usd=amount_usd,
        side=side,
        price_quote=snapshot["price_quote"],
    )

    market_order = MarketOrderArgs(
        token_id=token_id,
        amount=order_amount,
        side=_side_constant(side),
        order_type=OrderType.FOK,
    )

    signed = client.create_market_order(market_order)

    return {
        "token_id": token_id,
        "amount_usd": amount_usd,
        "order_amount": order_amount,
        "order_amount_units": "usdc" if side.upper() == "BUY" else "shares",
        "side": side,
        "midpoint": snapshot["midpoint"],
        "price_quote": snapshot["price_quote"],
        "best_bid": snapshot["best_bid"],
        "best_ask": snapshot["best_ask"],
        "spread": snapshot["spread"],
        "min_order_size": snapshot.get("min_order_size"),
        "tick_size": snapshot.get("tick_size"),
        "neg_risk": snapshot.get("neg_risk"),
        "signed_order_type": type(signed).__name__,
        "signed_order_preview": str(signed)[:500],
    }


def submit_live_market_order(
    token_id: str,
    amount_usd: float,
    side: str = "BUY",
) -> dict:
    client = build_authenticated_client()
    snapshot = fetch_market_snapshot(token_id=token_id, side=side)
    order_amount = _market_order_amount(
        amount_usd=amount_usd,
        side=side,
        price_quote=snapshot["price_quote"],
    )

    market_order = MarketOrderArgs(
        token_id=token_id,
        amount=order_amount,
        side=_side_constant(side),
        order_type=OrderType.FOK,
    )

    signed = client.create_market_order(market_order)
    response = client.post_order(signed, OrderType.FOK)
    if not isinstance(response, dict):
        response = {"raw_response": response}

    return {
        "token_id": token_id,
        "amount_usd": amount_usd,
        "order_amount": order_amount,
        "order_amount_units": "usdc" if side.upper() == "BUY" else "shares",
        "side": side,
        "midpoint": snapshot["midpoint"],
        "price_quote": snapshot["price_quote"],
        "best_bid": snapshot["best_bid"],
        "best_ask": snapshot["best_ask"],
        "spread": snapshot["spread"],
        "min_order_size": snapshot.get("min_order_size"),
        "tick_size": snapshot.get("tick_size"),
        "neg_risk": snapshot.get("neg_risk"),
        "order_type": "FOK",
        "signed_order_type": type(signed).__name__,
        "post_order_response": response,
    }


def fetch_live_order_status(order_id: str) -> dict:
    client = build_authenticated_client()
    response = client.get_order(order_id)
    if not isinstance(response, dict):
        response = {"raw_response": response}
    return response
