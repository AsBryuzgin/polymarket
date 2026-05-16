from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

from execution.state_store import get_connection


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
    except ValueError:
        try:
            dt = datetime.fromisoformat(str(value).replace(" ", "T") + "+00:00")
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _best_bid_ask_from_book(message: dict[str, Any]) -> tuple[float | None, float | None]:
    bids = [
        price
        for price in (_safe_float(level.get("price")) for level in message.get("bids") or [])
        if price is not None
    ]
    asks = [
        price
        for price in (_safe_float(level.get("price")) for level in message.get("asks") or [])
        if price is not None
    ]
    best_bid = max(bids) if bids else None
    best_ask = min(asks) if asks else None
    return best_bid, best_ask


def _normalize_ws_message(message: dict[str, Any]) -> dict[str, Any] | None:
    token_id = str(message.get("asset_id") or message.get("assetId") or "").strip()
    if not token_id:
        return None

    event_type = str(message.get("event_type") or message.get("eventType") or "").strip()
    best_bid = _safe_float(message.get("best_bid"))
    best_ask = _safe_float(message.get("best_ask"))
    spread = _safe_float(message.get("spread"))
    last_trade_price = _safe_float(message.get("price"))

    if event_type == "book":
        best_bid, best_ask = _best_bid_ask_from_book(message)
    elif event_type == "price_change":
        for change in message.get("price_changes") or []:
            if str(change.get("asset_id") or change.get("assetId") or token_id) != token_id:
                continue
            best_bid = _safe_float(change.get("best_bid")) or best_bid
            best_ask = _safe_float(change.get("best_ask")) or best_ask
    elif event_type == "last_trade_price":
        last_trade_price = _safe_float(message.get("price"))

    if spread is None and best_bid is not None and best_ask is not None:
        spread = round(best_ask - best_bid, 8)

    midpoint = None
    if best_bid is not None and best_ask is not None:
        midpoint = round((best_bid + best_ask) / 2.0, 8)

    return {
        "token_id": token_id,
        "market": message.get("market"),
        "event_type": event_type or None,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "midpoint": midpoint,
        "last_trade_price": last_trade_price,
        "raw_json": json.dumps(message, sort_keys=True),
    }


def init_market_cache_table() -> None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS market_snapshot_cache (
            token_id TEXT PRIMARY KEY,
            market TEXT,
            event_type TEXT,
            midpoint REAL,
            price_quote_buy REAL,
            price_quote_sell REAL,
            best_bid REAL,
            best_ask REAL,
            spread REAL,
            last_trade_price REAL,
            raw_json TEXT,
            observed_at TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()
    conn.close()


def upsert_market_cache_from_ws(message: dict[str, Any]) -> bool:
    normalized = _normalize_ws_message(message)
    if normalized is None:
        return False

    observed_at = _utc_now_iso()
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO market_snapshot_cache (
            token_id,
            market,
            event_type,
            midpoint,
            price_quote_buy,
            price_quote_sell,
            best_bid,
            best_ask,
            spread,
            last_trade_price,
            raw_json,
            observed_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(token_id) DO UPDATE SET
            market = COALESCE(excluded.market, market_snapshot_cache.market),
            event_type = COALESCE(excluded.event_type, market_snapshot_cache.event_type),
            midpoint = COALESCE(excluded.midpoint, market_snapshot_cache.midpoint),
            price_quote_buy = COALESCE(excluded.price_quote_buy, market_snapshot_cache.price_quote_buy),
            price_quote_sell = COALESCE(excluded.price_quote_sell, market_snapshot_cache.price_quote_sell),
            best_bid = COALESCE(excluded.best_bid, market_snapshot_cache.best_bid),
            best_ask = COALESCE(excluded.best_ask, market_snapshot_cache.best_ask),
            spread = COALESCE(excluded.spread, market_snapshot_cache.spread),
            last_trade_price = COALESCE(excluded.last_trade_price, market_snapshot_cache.last_trade_price),
            raw_json = excluded.raw_json,
            observed_at = excluded.observed_at,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            normalized["token_id"],
            normalized["market"],
            normalized["event_type"],
            normalized["midpoint"],
            normalized["best_ask"],
            normalized["best_bid"],
            normalized["best_bid"],
            normalized["best_ask"],
            normalized["spread"],
            normalized["last_trade_price"],
            normalized["raw_json"],
            observed_at,
        ),
    )
    conn.commit()
    conn.close()
    return True


def get_market_cache_snapshot(token_id: str, *, side: str = "BUY", max_age_sec: float = 5.0) -> dict[str, Any] | None:
    init_market_cache_table()
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM market_snapshot_cache
        WHERE token_id = ?
        LIMIT 1
        """,
        (token_id,),
    )
    row = cur.fetchone()
    conn.close()
    if row is None:
        return None

    payload = dict(row)
    observed_at = _parse_time(payload.get("observed_at"))
    if observed_at is None:
        return None
    age_sec = (datetime.now(timezone.utc) - observed_at).total_seconds()
    if age_sec > max_age_sec:
        return None

    price_quote = payload.get("price_quote_buy") if side.upper() == "BUY" else payload.get("price_quote_sell")
    if price_quote is None:
        price_quote = payload.get("best_ask") if side.upper() == "BUY" else payload.get("best_bid")

    return {
        "token_id": token_id,
        "side": side,
        "midpoint": payload.get("midpoint"),
        "price_quote": price_quote,
        "best_bid": payload.get("best_bid"),
        "best_ask": payload.get("best_ask"),
        "spread": payload.get("spread"),
        "min_order_size": None,
        "tick_size": None,
        "neg_risk": None,
        "last_trade_price": payload.get("last_trade_price"),
        "source": "market_ws_cache",
        "cache_age_sec": round(age_sec, 3),
        "raw_ws_cache": payload.get("raw_json"),
        "raw_midpoint": None,
        "raw_price_quote": None,
    }


def list_market_cache_token_ids(*, recent_minutes: int = 360, max_tokens: int = 250) -> list[str]:
    init_market_cache_table()
    conn = get_connection()
    cur = conn.cursor()
    tokens: set[str] = set()

    cur.execute(
        """
        SELECT token_id
        FROM copied_positions
        WHERE status = 'OPEN' AND token_id IS NOT NULL AND token_id != ''
        """
    )
    tokens.update(str(row["token_id"]) for row in cur.fetchall())

    try:
        cur.execute(
            """
            SELECT token_id, latest_token_id
            FROM signal_observations
            WHERE observed_at >= datetime('now', ?)
            ORDER BY observed_at DESC
            LIMIT ?
            """,
            (f"-{int(recent_minutes)} minutes", max_tokens * 4),
        )
        for row in cur.fetchall():
            for key in ("token_id", "latest_token_id"):
                value = row[key]
                if value:
                    tokens.add(str(value))
    except sqlite3.OperationalError:
        pass

    conn.close()
    return sorted(tokens)[:max_tokens]


def market_cache_summary(*, max_age_sec: float = 5.0) -> dict[str, Any]:
    init_market_cache_table()
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS total FROM market_snapshot_cache")
    total = int(cur.fetchone()["total"])
    cur.execute(
        """
        SELECT COUNT(*) AS fresh
        FROM market_snapshot_cache
        WHERE (julianday('now') - julianday(observed_at)) * 86400.0 <= ?
        """,
        (float(max_age_sec),),
    )
    fresh = int(cur.fetchone()["fresh"])
    cur.execute(
        """
        SELECT token_id, event_type, best_bid, best_ask, spread, observed_at
        FROM market_snapshot_cache
        ORDER BY observed_at DESC
        LIMIT 5
        """
    )
    latest = [dict(row) for row in cur.fetchall()]
    conn.close()
    return {
        "total_tokens": total,
        "fresh_tokens": fresh,
        "max_age_sec": max_age_sec,
        "latest": latest,
    }
