from __future__ import annotations

from typing import Any
import requests

from app.config import settings


class WalletProfilesClient:
    def __init__(
        self,
        gamma_base_url: str | None = None,
        data_base_url: str | None = None,
        timeout: int = 20,
    ) -> None:
        self.gamma_base_url = (gamma_base_url or settings.gamma_base_url).rstrip("/")
        self.data_base_url = (data_base_url or settings.data_base_url).rstrip("/")
        self.timeout = timeout

    def _get_gamma(self, path: str, params: dict[str, Any]) -> Any:
        url = f"{self.gamma_base_url}/{path.lstrip('/')}"
        response = requests.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def _get_data(self, path: str, params: dict[str, Any]) -> Any:
        url = f"{self.data_base_url}/{path.lstrip('/')}"
        response = requests.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def get_public_profile(self, address: str) -> dict[str, Any]:
        return self._get_gamma("/public-profile", {"address": address})

    def get_total_markets_traded(self, user: str) -> dict[str, Any]:
        return self._get_data("/traded", {"user": user})

    def get_current_positions(
        self,
        user: str,
        limit: int = 100,
        offset: int = 0,
        sort_by: str = "TOKENS",
        sort_direction: str = "DESC",
    ) -> list[dict[str, Any]]:
        return self._get_data(
            "/positions",
            {
                "user": user,
                "limit": limit,
                "offset": offset,
                "sortBy": sort_by,
                "sortDirection": sort_direction,
            },
        )

    def get_closed_positions(
        self,
        user: str,
        limit: int = 50,
        offset: int = 0,
        sort_by: str = "REALIZEDPNL",
        sort_direction: str = "DESC",
    ) -> list[dict[str, Any]]:
        return self._get_data(
            "/closed-positions",
            {
                "user": user,
                "limit": limit,
                "offset": offset,
                "sortBy": sort_by,
                "sortDirection": sort_direction,
            },
        )

    def get_trades(
        self,
        user: str,
        limit: int = 100,
        offset: int = 0,
        taker_only: bool = True,
    ) -> list[dict[str, Any]]:
        return self._get_data(
            "/trades",
            {
                "user": user,
                "limit": limit,
                "offset": offset,
                "takerOnly": str(taker_only).lower(),
            },
        )

    @staticmethod
    def summarize_profile(profile: dict[str, Any]) -> dict[str, Any]:
        return {
            "proxy_wallet": profile.get("proxyWallet"),
            "name": profile.get("name"),
            "pseudonym": profile.get("pseudonym"),
            "x_username": profile.get("xUsername"),
            "verified_badge": bool(profile.get("verifiedBadge", False)),
            "created_at": profile.get("createdAt"),
        }

    @staticmethod
    def summarize_total_markets_traded(payload: dict[str, Any]) -> int:
        return int(payload.get("traded", 0) or 0)

    @staticmethod
    def summarize_positions(positions: list[dict[str, Any]]) -> dict[str, Any]:
        total_current_value = 0.0
        total_cash_pnl = 0.0

        for item in positions:
            total_current_value += float(item.get("currentValue", 0) or 0)
            total_cash_pnl += float(item.get("cashPnl", 0) or 0)

        return {
            "open_positions_count": len(positions),
            "open_current_value": round(total_current_value, 2),
            "open_cash_pnl": round(total_cash_pnl, 2),
        }

    @staticmethod
    def summarize_closed_positions(positions: list[dict[str, Any]]) -> dict[str, Any]:
        total_realized_pnl = 0.0
        total_bought = 0.0

        for item in positions:
            total_realized_pnl += float(item.get("realizedPnl", 0) or 0)
            total_bought += float(item.get("totalBought", 0) or 0)

        return {
            "closed_positions_count": len(positions),
            "closed_realized_pnl_sum": round(total_realized_pnl, 2),
            "closed_total_bought_sum": round(total_bought, 2),
        }

    @staticmethod
    def summarize_trades(trades: list[dict[str, Any]]) -> dict[str, Any]:
        total_notional = 0.0
        buy_count = 0
        sell_count = 0

        for item in trades:
            size = float(item.get("size", 0) or 0)
            price = float(item.get("price", 0) or 0)
            total_notional += size * price

            side = str(item.get("side", "")).upper()
            if side == "BUY":
                buy_count += 1
            elif side == "SELL":
                sell_count += 1

        return {
            "trade_count": len(trades),
            "buy_count": buy_count,
            "sell_count": sell_count,
            "trade_notional_sum": round(total_notional, 2),
        }
