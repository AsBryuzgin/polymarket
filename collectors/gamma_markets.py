from __future__ import annotations

import json
from typing import Any

import requests

from app.config import settings


class GammaMarketsClient:
    def __init__(self, base_url: str | None = None, timeout: int = 15) -> None:
        self.base_url = (base_url or settings.gamma_base_url).rstrip("/")
        self.timeout = timeout

    def _get(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        response = requests.get(
            f"{self.base_url}/{path.lstrip('/')}",
            params=params,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _extract_markets(data: Any) -> list[dict[str, Any]]:
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]

        if isinstance(data, dict):
            for key in ("markets", "data", "items"):
                value = data.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]

        raise ValueError("Unexpected Gamma API response format")

    def list_markets(self, **params: Any) -> list[dict[str, Any]]:
        return self._extract_markets(self._get("markets", params=params))

    def get_markets(self, limit: int = 10, active: bool = True, closed: bool = False) -> list[dict[str, Any]]:
        params = {
            "limit": limit,
            "active": str(active).lower(),
            "closed": str(closed).lower(),
        }
        return self.list_markets(**params)

    def get_market_by_token_id(self, token_id: str) -> dict[str, Any] | None:
        token_id = str(token_id).strip()
        if not token_id:
            return None

        for params in (
            {"limit": 5, "clob_token_ids": token_id},
            {"limit": 5, "clob_token_ids": token_id, "closed": "true"},
        ):
            rows = self.list_markets(**params)
            if rows:
                return rows[0]
        return None

    def get_market_by_condition_id(self, condition_id: str) -> dict[str, Any] | None:
        condition_id = str(condition_id).strip()
        if not condition_id:
            return None

        for params in (
            {"limit": 5, "condition_ids": condition_id},
            {"limit": 5, "condition_ids": condition_id, "closed": "true"},
        ):
            rows = self.list_markets(**params)
            if rows:
                return rows[0]
        return None

    @staticmethod
    def _parse_clob_token_ids(raw_value: Any) -> list[str]:
        if raw_value is None:
            return []

        if isinstance(raw_value, list):
            return [str(x) for x in raw_value if x is not None]

        if isinstance(raw_value, str):
            raw_value = raw_value.strip()

            if raw_value.startswith("[") and raw_value.endswith("]"):
                try:
                    parsed = json.loads(raw_value)
                    if isinstance(parsed, list):
                        return [str(x) for x in parsed if x is not None]
                except json.JSONDecodeError:
                    pass

            return [raw_value]

        return []

    @classmethod
    def extract_tokens(cls, market: dict[str, Any]) -> dict[str, str | None]:
        clob_token_ids = cls._parse_clob_token_ids(market.get("clobTokenIds"))

        yes_token_id = clob_token_ids[0] if len(clob_token_ids) > 0 else None
        no_token_id = clob_token_ids[1] if len(clob_token_ids) > 1 else None

        return {
            "yes_token_id": yes_token_id,
            "no_token_id": no_token_id,
        }

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        try:
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    @classmethod
    def normalize_market(cls, market: dict[str, Any]) -> dict[str, Any]:
        tokens = cls.extract_tokens(market)

        return {
            "id": market.get("id"),
            "condition_id": market.get("conditionId") or market.get("condition_id"),
            "slug": market.get("slug"),
            "question": market.get("question") or market.get("title") or "N/A",
            "active": market.get("active"),
            "closed": market.get("closed"),
            "archived": market.get("archived"),
            "accepting_orders": market.get("acceptingOrders"),
            "liquidity": cls._safe_float(market.get("liquidity")),
            "volume": cls._safe_float(market.get("volume")),
            "end_date": market.get("endDate") or market.get("end_date"),
            "enable_order_book": market.get("enableOrderBook"),
            "uma_resolution_status": market.get("umaResolutionStatus"),
            "best_bid": cls._safe_float(market.get("bestBid")),
            "best_ask": cls._safe_float(market.get("bestAsk")),
            "last_trade_price": cls._safe_float(market.get("lastTradePrice")),
            "yes_token_id": tokens["yes_token_id"],
            "no_token_id": tokens["no_token_id"],
            "tokens": market.get("tokens"),
        }
