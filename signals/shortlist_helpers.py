from __future__ import annotations

from datetime import datetime, timezone
from statistics import median
from typing import Any

from execution.polymarket_executor import fetch_market_snapshot


def _parse_ts(value: Any) -> datetime | None:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        try:
            v = float(value)
            if v > 10_000_000_000:
                v = v / 1000.0
            return datetime.fromtimestamp(v, tz=timezone.utc)
        except Exception:
            return None

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None

        if s.isdigit():
            try:
                v = float(s)
                if v > 10_000_000_000:
                    v = v / 1000.0
                return datetime.fromtimestamp(v, tz=timezone.utc)
            except Exception:
                pass

        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return None

    return None


def extract_closed_position_ts(pos: dict[str, Any]) -> datetime | None:
    candidates = [
        pos.get("closedAt"),
        pos.get("closeTime"),
        pos.get("closeTimestamp"),
        pos.get("timestamp"),
        pos.get("updatedAt"),
        pos.get("createdAt"),
        pos.get("endDate"),
        pos.get("end_time"),
    ]

    for candidate in candidates:
        dt = _parse_ts(candidate)
        if dt is not None:
            return dt

    return None


def position_dedupe_key(pos: dict[str, Any]) -> str:
    return str(
        pos.get("id")
        or pos.get("positionId")
        or pos.get("transactionHash")
        or pos.get("orderId")
        or f"{pos.get('asset')}|{pos.get('slug')}|{pos.get('outcome')}|{pos.get('size')}|{pos.get('price')}"
    )


def paginate_recent_closed_positions(
    wallet_client,
    wallet: str,
    page_size: int = 100,
    max_pages: int = 10,
) -> list[dict[str, Any]]:
    all_rows: list[dict[str, Any]] = []
    seen: set[str] = set()

    for page in range(max_pages):
        offset = page * page_size

        try:
            rows = wallet_client.get_closed_positions(
                user=wallet,
                limit=page_size,
                offset=offset,
                sort_by="CLOSETIME",
                sort_direction="DESC",
            )
        except Exception:
            try:
                rows = wallet_client.get_closed_positions(
                    user=wallet,
                    limit=page_size,
                    offset=offset,
                    sort_by="TIMESTAMP",
                    sort_direction="DESC",
                )
            except Exception:
                rows = wallet_client.get_closed_positions(
                    user=wallet,
                    limit=page_size,
                    offset=offset,
                )

        if not rows:
            break

        for row in rows:
            key = position_dedupe_key(row)
            if key in seen:
                continue
            seen.add(key)
            all_rows.append(row)

        if len(rows) < page_size:
            break

    all_rows.sort(
        key=lambda x: extract_closed_position_ts(x) or datetime(1970, 1, 1, tzinfo=timezone.utc),
        reverse=True,
    )
    return all_rows


def _safe_float(x: Any) -> float:
    try:
        return float(x) if x is not None else 0.0
    except Exception:
        return 0.0


def _median_or(values: list[float], fallback: float) -> float:
    clean = [float(v) for v in values if v is not None]
    return float(median(clean)) if clean else float(fallback)


def _extract_token_ids(current_positions: list[dict[str, Any]], trades: list[dict[str, Any]], max_tokens: int = 12) -> list[str]:
    token_ids: list[str] = []
    seen: set[str] = set()

    def add_token(raw: Any) -> None:
        token = str(raw or "").strip()
        if not token or token in seen:
            return
        seen.add(token)
        token_ids.append(token)

    for row in trades:
        add_token(row.get("asset"))
        add_token(row.get("token_id"))
        if len(token_ids) >= max_tokens:
            return token_ids

    for row in current_positions:
        add_token(row.get("asset"))
        add_token(row.get("token_id"))
        if len(token_ids) >= max_tokens:
            return token_ids

    return token_ids


def estimate_copyability_inputs(
    current_positions: list[dict[str, Any]],
    trades: list[dict[str, Any]],
) -> tuple[float, float, float, float]:
    token_ids = _extract_token_ids(current_positions, trades, max_tokens=12)

    spreads: list[float] = []
    trade_notionals: list[float] = []

    for trade in trades[:50]:
        size = _safe_float(trade.get("size"))
        price = _safe_float(trade.get("price"))
        notional = size * price
        if notional > 0:
            trade_notionals.append(notional)

    valid_snapshots = 0
    for token_id in token_ids:
        try:
            snapshot = fetch_market_snapshot(token_id=token_id, side="BUY")
            spread = _safe_float(snapshot.get("spread"))
            if spread > 0:
                spreads.append(spread)
            valid_snapshots += 1
        except Exception:
            continue

    coverage = (valid_snapshots / len(token_ids)) if token_ids else 0.0
    median_spread = _median_or(spreads, 0.03)
    median_trade_notional = _median_or(trade_notionals, 50.0)

    # Proxy, not true book depth:
    # higher recently executed notional + better orderbook coverage => more copyable market set.
    median_liquidity = min(50000.0, max(500.0, median_trade_notional * 100.0 * max(coverage, 0.25)))

    # Wider spreads / poor coverage imply worse execution quality.
    slippage_proxy = min(0.05, max(0.002, median_spread * (0.5 + (1.0 - coverage))))

    # Keep delay conservative until we wire actual measured pipeline latency.
    delay_sec = 60.0

    return (
        round(median_spread, 6),
        round(median_liquidity, 2),
        round(slippage_proxy, 6),
        round(delay_sec, 2),
    )
