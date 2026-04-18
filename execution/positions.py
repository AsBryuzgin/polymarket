from __future__ import annotations

from typing import Any

from py_clob_client.clob_types import OpenOrderParams

from collectors.wallet_profiles import WalletProfilesClient
from execution.polymarket_executor import build_authenticated_client


def _safe_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _first_present(item: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = item.get(key)
        if value not in (None, ""):
            return value
    return None


def normalize_exchange_position(item: dict[str, Any]) -> dict[str, Any] | None:
    token_id = _first_present(
        item,
        (
            "asset",
            "assetId",
            "asset_id",
            "token_id",
            "tokenId",
            "outcomeTokenId",
        ),
    )
    if token_id is None:
        return None

    size = _safe_float(
        _first_present(
            item,
            (
                "size",
                "tokens",
                "shares",
                "quantity",
                "balance",
            ),
        )
    )

    current_value = _safe_float(
        _first_present(
            item,
            (
                "currentValue",
                "current_value",
                "value",
                "marketValue",
            ),
        )
    )

    avg_price = _safe_float(
        _first_present(
            item,
            (
                "avgPrice",
                "avg_price",
                "averagePrice",
                "price",
            ),
        )
    )

    return {
        "token_id": str(token_id),
        "size": size,
        "current_value_usd": current_value,
        "avg_price": avg_price,
        "condition_id": _first_present(item, ("conditionId", "condition_id")),
        "market_slug": _first_present(item, ("slug", "marketSlug", "market_slug")),
        "outcome": _first_present(item, ("outcome", "outcomeName")),
        "raw": item,
    }


def normalize_exchange_open_order(item: dict[str, Any]) -> dict[str, Any] | None:
    token_id = _first_present(
        item,
        (
            "asset_id",
            "assetId",
            "token_id",
            "tokenId",
            "asset",
        ),
    )
    if token_id is None:
        return None

    original_size = _safe_float(
        _first_present(item, ("original_size", "originalSize", "size", "amount"))
    )
    size_matched = _safe_float(
        _first_present(item, ("size_matched", "sizeMatched", "matched_size"))
    )
    remaining_size = None
    if original_size is not None:
        remaining_size = original_size - (size_matched or 0.0)

    return {
        "order_id": _first_present(item, ("id", "order_id", "orderId")),
        "token_id": str(token_id),
        "side": str(_first_present(item, ("side", "direction")) or "").upper(),
        "price": _safe_float(_first_present(item, ("price", "limit_price", "limitPrice"))),
        "original_size": original_size,
        "size_matched": size_matched,
        "remaining_size": remaining_size,
        "status": _first_present(item, ("status", "state")),
        "raw": item,
    }


def fetch_exchange_positions(
    user: str,
    *,
    client: WalletProfilesClient | None = None,
    page_size: int = 100,
    max_pages: int = 20,
) -> list[dict[str, Any]]:
    client = client or WalletProfilesClient()
    rows = client.paginate_current_positions(
        user=user,
        page_size=page_size,
        max_pages=max_pages,
    )
    normalized = [normalize_exchange_position(row) for row in rows]
    return [row for row in normalized if row is not None]


def fetch_exchange_open_orders(token_ids: list[str] | None = None) -> list[dict[str, Any]]:
    client = build_authenticated_client()

    if not token_ids:
        raw_orders = client.get_orders()
        if isinstance(raw_orders, dict) and isinstance(raw_orders.get("data"), list):
            raw_orders = raw_orders["data"]
        if not isinstance(raw_orders, list):
            raw_orders = []
        normalized = [normalize_exchange_open_order(row) for row in raw_orders]
        return [row for row in normalized if row is not None]

    orders: list[dict[str, Any]] = []
    for token_id in token_ids:
        raw_orders = client.get_orders(OpenOrderParams(asset_id=token_id))
        if isinstance(raw_orders, dict) and isinstance(raw_orders.get("data"), list):
            raw_orders = raw_orders["data"]
        if not isinstance(raw_orders, list):
            continue
        for row in raw_orders:
            normalized = normalize_exchange_open_order(row)
            if normalized is not None:
                orders.append(normalized)

    return orders
