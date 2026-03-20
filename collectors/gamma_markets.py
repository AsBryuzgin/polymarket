from __future__ import annotations

from typing import Any
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
    def normalize_market(market: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": market.get("id"),
            "question": market.get("question") or market.get("title") or "N/A",
            "active": market.get("active"),
            "closed": market.get("closed"),
            "liquidity": market.get("liquidity"),
            "volume": market.get("volume"),
            "end_date": market.get("endDate") or market.get("end_date"),
        }
