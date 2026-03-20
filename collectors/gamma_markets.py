from __future__ import annotations

from typing import Any
import json
import requests

from app.config import settings


class GammaMarketsClient:
    def __init__(self, base_url: str | None = None, timeout: int = 15) -> None:
        self.base_url = (base_url or settings.gamma_base_url).rstrip("/")
        self.timeout = timeout

    def get_markets(self, limit: int = 10, active: bool = True, closed: bool = False) -> list[dict[str, Any]]:
        url = f"{self.base_url}/markets"
        params = {
            "limit": limit,
            "active": str(active).lower(),
            "closed": str(closed).lower(),
        }

        response = requests.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        data = response.json()

        if isinstance(data, list):
            return data

        if isinstance(data, dict):
            for key in ("markets", "data", "items"):
                value = data.get(key)
                if isinstance(value, list):
                    return value

        raise ValueError("Unexpected Gamma API response format")

    @staticmethod
    def _parse_clob_token_ids(raw_value: Any) -> list[str]:
        if raw_value is None:
            return []

        if isinstance(raw_value, list):
            return [str(x) for x in raw_value if x is not None]

        if isinstance(raw_value, str):
            raw_value = raw_value.strip()

            # Иногда приходит JSON-строкой: '["123","456"]'
            if raw_value.startswith("[") and raw_value.endswith("]"):
                try:
                    parsed = json.loads(raw_value)
                    if isinstance(parsed, list):
                        return [str(x) for x in parsed if x is not None]
                except json.JSONDecodeError:
                    pass

            # fallback: просто одна строка
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

    @classmethod
    def normalize_market(cls, market: dict[str, Any]) -> dict[str, Any]:
        tokens = cls.extract_tokens(market)

        return {
            "id": market.get("id"),
            "question": market.get("question") or market.get("title") or "N/A",
            "active": market.get("active"),
            "closed": market.get("closed"),
            "liquidity": market.get("liquidity"),
            "volume": market.get("volume"),
            "end_date": market.get("endDate") or market.get("end_date"),
            "enable_order_book": market.get("enableOrderBook"),
            "yes_token_id": tokens["yes_token_id"],
            "no_token_id": tokens["no_token_id"],
        }
