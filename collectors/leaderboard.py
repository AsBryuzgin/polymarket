from __future__ import annotations

from typing import Any
import requests

from app.config import settings


class LeaderboardClient:
    def __init__(self, base_url: str | None = None, timeout: int = 20) -> None:
        self.base_url = (base_url or settings.data_base_url).rstrip("/")
        self.timeout = timeout

    def get_leaderboard(self) -> list[dict[str, Any]]:
        url = f"{self.base_url}/v1/leaderboard"
        response = requests.get(url, timeout=self.timeout)
        response.raise_for_status()

        data = response.json()
        if not isinstance(data, list):
            raise ValueError("Unexpected leaderboard response format")

        return data

    @staticmethod
    def normalize_entry(entry: dict[str, Any]) -> dict[str, Any]:
        return {
            "rank": int(entry.get("rank")) if entry.get("rank") is not None else None,
            "proxy_wallet": entry.get("proxyWallet"),
            "user_name": entry.get("userName"),
            "volume": float(entry.get("vol", 0) or 0),
            "pnl": float(entry.get("pnl", 0) or 0),
            "profile_image": entry.get("profileImage"),
            "x_username": entry.get("xUsername"),
            "verified_badge": bool(entry.get("verifiedBadge", False)),
        }
