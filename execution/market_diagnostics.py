from __future__ import annotations

from functools import lru_cache
from typing import Any

import requests

from app.config import settings
from collectors.gamma_markets import GammaMarketsClient


def _safe_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _safe_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes"}:
        return True
    if text in {"false", "0", "no"}:
        return False
    return None


def _first_present(item: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = item.get(key)
        if value not in (None, ""):
            return value
    return None


def _extract_token_meta(raw_tokens: Any, token_id: str) -> dict[str, Any]:
    if isinstance(raw_tokens, str):
        try:
            import json

            raw_tokens = json.loads(raw_tokens)
        except Exception:
            raw_tokens = None

    if not isinstance(raw_tokens, list):
        return {}

    for token in raw_tokens:
        if not isinstance(token, dict):
            continue
        candidate = _first_present(
            token,
            (
                "token_id",
                "tokenId",
                "clobTokenId",
                "clob_token_id",
                "id",
            ),
        )
        if candidate is not None and str(candidate) == token_id:
            return {
                "token_id": str(candidate),
                "outcome": _first_present(token, ("outcome", "name")),
                "winner": _safe_bool(_first_present(token, ("winner", "isWinner"))),
                "price": _safe_float(_first_present(token, ("price", "lastPrice"))),
            }
    return {}


def _maybe_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            import json

            parsed = json.loads(value)
        except Exception:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _extract_token_meta_from_market(raw_market: dict[str, Any], token_id: str) -> dict[str, Any]:
    token_meta = _extract_token_meta(raw_market.get("tokens"), token_id)
    if token_meta:
        return token_meta

    token_ids = [str(item) for item in _maybe_json_list(raw_market.get("clobTokenIds"))]
    outcomes = [str(item) for item in _maybe_json_list(raw_market.get("outcomes"))]
    outcome_prices = _maybe_json_list(raw_market.get("outcomePrices"))

    try:
        idx = token_ids.index(token_id)
    except ValueError:
        return {}

    price = None
    winner = None
    if idx < len(outcome_prices):
        price = _safe_float(outcome_prices[idx])
        if price is not None:
            if abs(price - 1.0) < 1e-9:
                winner = True
            elif abs(price - 0.0) < 1e-9:
                winner = False

    return {
        "token_id": token_id,
        "outcome": outcomes[idx] if idx < len(outcomes) else None,
        "winner": winner,
        "price": price,
    }


def _looks_resolved(status: Any) -> bool:
    text = str(status or "").strip().lower()
    if not text:
        return False
    return any(
        marker in text
        for marker in (
            "resolved",
            "final",
            "settled",
            "redeem",
            "reported",
            "resolved_yes",
            "resolved_no",
        )
    )


class ClobPublicMarketsClient:
    def __init__(self, base_url: str | None = None, timeout: int = 10) -> None:
        self.base_url = (base_url or settings.clob_base_url).rstrip("/")
        self.timeout = timeout

    def get_market_by_token_id(self, token_id: str) -> dict[str, Any] | None:
        response = requests.get(
            f"{self.base_url}/markets-by-token/{token_id}",
            timeout=self.timeout,
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, dict) else None


@lru_cache(maxsize=2048)
def lookup_token_market(token_id: str) -> dict[str, Any] | None:
    token_id = str(token_id).strip()
    if not token_id:
        return None

    gamma = GammaMarketsClient(timeout=10)
    raw_market = gamma.get_market_by_token_id(token_id)
    condition_id = None

    if raw_market is None:
        clob = ClobPublicMarketsClient(timeout=10)
        clob_market = clob.get_market_by_token_id(token_id)
        if clob_market is not None:
            condition_id = str(
                _first_present(
                    clob_market,
                    ("condition_id", "conditionId", "market", "market_id"),
                )
                or ""
            ).strip()
            if condition_id:
                raw_market = gamma.get_market_by_condition_id(condition_id)
    else:
        condition_id = str(raw_market.get("conditionId") or raw_market.get("condition_id") or "").strip()

    if raw_market is None:
        return None

    market = GammaMarketsClient.normalize_market(raw_market)
    if condition_id and not market.get("condition_id"):
        market["condition_id"] = condition_id

    token_meta = _extract_token_meta_from_market(raw_market, token_id)
    market.update(
        {
            "token_id": token_id,
            "token_outcome": token_meta.get("outcome"),
            "token_winner": token_meta.get("winner"),
            "token_last_price": token_meta.get("price"),
        }
    )
    return market


def diagnose_market_snapshot_error(token_id: str, error_message: str) -> dict[str, Any]:
    token_id = str(token_id).strip()
    error_message = str(error_message or "").strip()
    diagnosis: dict[str, Any] = {
        "token_id": token_id,
        "error_message": error_message,
        "diagnosis_status": "SNAPSHOT_ERROR",
        "diagnosis_label": "snapshot error",
        "diagnosis_reason": error_message or "snapshot failed",
        "market_found": False,
        "question": None,
        "slug": None,
        "condition_id": None,
        "active": None,
        "closed": None,
        "archived": None,
        "accepting_orders": None,
        "enable_order_book": None,
        "uma_resolution_status": None,
        "token_outcome": None,
        "token_winner": None,
        "token_last_price": None,
        "best_bid": None,
        "best_ask": None,
        "last_trade_price": None,
        "action_hint": "check raw snapshot error",
    }

    if "no orderbook exists" not in error_message.lower() and "404" not in error_message:
        return diagnosis

    diagnosis["diagnosis_status"] = "NO_ORDERBOOK"
    diagnosis["diagnosis_label"] = "no orderbook"
    diagnosis["diagnosis_reason"] = "CLOB does not expose an orderbook for this token"
    diagnosis["action_hint"] = "inspect token market state"

    try:
        market = lookup_token_market(token_id)
    except Exception as exc:
        diagnosis["diagnosis_status"] = "NO_ORDERBOOK_LOOKUP_ERROR"
        diagnosis["diagnosis_label"] = "lookup error"
        diagnosis["diagnosis_reason"] = f"market lookup failed: {exc}"
        diagnosis["action_hint"] = "retry lookup or inspect Gamma/CLOB APIs"
        return diagnosis

    if market is None:
        diagnosis["diagnosis_status"] = "NO_ORDERBOOK_MARKET_NOT_FOUND"
        diagnosis["diagnosis_label"] = "market not found"
        diagnosis["diagnosis_reason"] = "token lookup returned no Gamma/CLOB market"
        diagnosis["action_hint"] = "treat as suspicious and inspect token manually"
        return diagnosis

    diagnosis.update(
        {
            "market_found": True,
            "question": market.get("question"),
            "slug": market.get("slug"),
            "condition_id": market.get("condition_id"),
            "active": _safe_bool(market.get("active")),
            "closed": _safe_bool(market.get("closed")),
            "archived": _safe_bool(market.get("archived")),
            "accepting_orders": _safe_bool(market.get("accepting_orders")),
            "enable_order_book": _safe_bool(market.get("enable_order_book")),
            "uma_resolution_status": market.get("uma_resolution_status"),
            "token_outcome": market.get("token_outcome"),
            "token_winner": market.get("token_winner"),
            "token_last_price": _safe_float(market.get("token_last_price")),
            "best_bid": _safe_float(market.get("best_bid")),
            "best_ask": _safe_float(market.get("best_ask")),
            "last_trade_price": _safe_float(market.get("last_trade_price")),
        }
    )

    if diagnosis["closed"] is True or diagnosis["archived"] is True or _looks_resolved(diagnosis["uma_resolution_status"]):
        diagnosis["diagnosis_status"] = "NO_ORDERBOOK_CLOSED_OR_RESOLVED"
        diagnosis["diagnosis_label"] = "closed/resolved"
        diagnosis["diagnosis_reason"] = "market is closed, archived, or already in a resolved state"
        diagnosis["action_hint"] = "CLOB mark is unavailable; settlement/redeem path is needed"
        return diagnosis

    if diagnosis["enable_order_book"] is False:
        diagnosis["diagnosis_status"] = "NO_ORDERBOOK_DISABLED"
        diagnosis["diagnosis_label"] = "orderbook disabled"
        diagnosis["diagnosis_reason"] = "market exists but enableOrderBook=false"
        diagnosis["action_hint"] = "do not expect CLOB quotes for this token"
        return diagnosis

    if diagnosis["accepting_orders"] is False:
        diagnosis["diagnosis_status"] = "NO_ORDERBOOK_NOT_ACCEPTING_ORDERS"
        diagnosis["diagnosis_label"] = "orders disabled"
        diagnosis["diagnosis_reason"] = "market exists but is not accepting new orders"
        diagnosis["action_hint"] = "treat as non-tradable until market state changes"
        return diagnosis

    if diagnosis["active"] is False:
        diagnosis["diagnosis_status"] = "NO_ORDERBOOK_INACTIVE"
        diagnosis["diagnosis_label"] = "inactive market"
        diagnosis["diagnosis_reason"] = "market is inactive and has no orderbook"
        diagnosis["action_hint"] = "treat as non-tradable and monitor resolution path"
        return diagnosis

    diagnosis["diagnosis_status"] = "NO_ORDERBOOK_ACTIVE_MARKET"
    diagnosis["diagnosis_label"] = "active market without book"
    diagnosis["diagnosis_reason"] = "market still looks active, but CLOB orderbook is unavailable"
    diagnosis["action_hint"] = "treat as suspicious; inspect token manually before live trading"
    return diagnosis
