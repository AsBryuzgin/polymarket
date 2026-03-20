from __future__ import annotations

from typing import Any
import requests

from app.config import settings


class ClobPricesClient:
    def __init__(self, base_url: str | None = None, timeout: int = 15) -> None:
        self.base_url = (base_url or settings.clob_base_url).rstrip("/")
        self.timeout = timeout

    def _get(self, path: str, token_id: str) -> dict[str, Any]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        response = requests.get(url, params={"token_id": token_id}, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def get_midpoint_raw(self, token_id: str) -> dict[str, Any]:
        return self._get("/midpoint", token_id)

    def get_spread_raw(self, token_id: str) -> dict[str, Any]:
        return self._get("/spread", token_id)

    def get_midpoint(self, token_id: str) -> str | None:
        data = self.get_midpoint_raw(token_id)
        return data.get("mid")

    def get_spread(self, token_id: str) -> str | None:
        data = self.get_spread_raw(token_id)
        return data.get("spread")
