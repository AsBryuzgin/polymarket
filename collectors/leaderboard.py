from __future__ import annotations

from typing import Any
import requests

from app.config import settings


class LeaderboardClient:
    VALID_CATEGORIES = {
        "OVERALL",
        "POLITICS",
        "SPORTS",
        "CRYPTO",
        "CULTURE",
        "MENTIONS",
        "WEATHER",
        "ECONOMICS",
        "TECH",
        "FINANCE",
    }

    VALID_TIME_PERIODS = {"DAY", "WEEK", "MONTH", "ALL"}
    VALID_ORDER_BY = {"PNL", "VOL"}

    def __init__(self, base_url: str | None = None, timeout: int = 20) -> None:
        self.base_url = (base_url or settings.data_base_url).rstrip("/")
        self.timeout = timeout

    def get_leaderboard(
        self,
        category: str = "OVERALL",
        time_period: str = "MONTH",
        order_by: str = "PNL",
        limit: int = 25,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        category = category.upper()
        time_period = time_period.upper()
        order_by = order_by.upper()

        if category not in self.VALID_CATEGORIES:
            raise ValueError(f"Invalid category: {category}")
        if time_period not in self.VALID_TIME_PERIODS:
            raise ValueError(f"Invalid time_period: {time_period}")
        if order_by not in self.VALID_ORDER_BY:
            raise ValueError(f"Invalid order_by: {order_by}")

        url = f"{self.base_url}/v1/leaderboard"
        params = {
            "category": category,
            "timePeriod": time_period,
            "orderBy": order_by,
            "limit": limit,
            "offset": offset,
        }

        response = requests.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()

        data = response.json()
        if not isinstance(data, list):
            raise ValueError("Unexpected leaderboard response format")

        return data

    @staticmethod
    def normalize_entry(entry: dict[str, Any], category: str | None = None, time_period: str | None = None) -> dict[str, Any]:
        return {
            "rank": int(entry.get("rank")) if entry.get("rank") is not None else None,
            "proxy_wallet": entry.get("proxyWallet"),
            "user_name": entry.get("userName"),
            "volume": float(entry.get("vol", 0) or 0),
            "pnl": float(entry.get("pnl", 0) or 0),
            "profile_image": entry.get("profileImage"),
            "x_username": entry.get("xUsername"),
            "verified_badge": bool(entry.get("verifiedBadge", False)),
            "leaderboard_category": category,
            "leaderboard_time_period": time_period,
        }
