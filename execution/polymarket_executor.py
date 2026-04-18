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

    market_order = MarketOrderArgs(
        token_id=token_id,
        amount=amount_usd,
        side=_side_constant(side),
        order_type=OrderType.FOK,
    )

    signed = client.create_market_order(market_order)

    return {
        "token_id": token_id,
        "amount_usd": amount_usd,
        "side": side,
        "midpoint": snapshot["midpoint"],
        "price_quote": snapshot["price_quote"],
        "best_bid": snapshot["best_bid"],
        "best_ask": snapshot["best_ask"],
        "spread": snapshot["spread"],
        "signed_order_type": type(signed).__name__,
        "signed_order_preview": str(signed)[:500],
    }


def submit_live_market_order(
    token_id: str,
    amount_usd: float,
    side: str = "BUY",
) -> dict:
    client = build_authenticated_client()

    market_order = MarketOrderArgs(
        token_id=token_id,
        amount=amount_usd,
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
        "side": side,
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
