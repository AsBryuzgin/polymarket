from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import datetime, timezone, timedelta
from typing import Any

from signals.domain_classifier import classify_domain
from signals.wallet_scoring import WalletMetrics


def _parse_iso_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value.replace("Z", "+00:00")
        return datetime.fromisoformat(value)
    except Exception:
        return None


def _parse_unix_ts(value: Any) -> datetime | None:
    try:
        if value is None:
            return None
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    except Exception:
        return None


def _item_dt(item: dict[str, Any]) -> datetime | None:
    for key in ("timestamp", "createdAt", "created_at", "updatedAt", "closedAt"):
        value = item.get(key)
        if value is None:
            continue
        if key == "timestamp":
            dt = _parse_unix_ts(value)
        else:
            dt = _parse_iso_dt(str(value))
        if dt is not None:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
    return None


def _safe_float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def _market_key(item: dict[str, Any]) -> str | None:
    for key in ("slug", "conditionId", "eventSlug", "asset"):
        value = item.get(key)
        if value is not None:
            return str(value)
    return None


def _item_domain(item: dict[str, Any]) -> str:
    return classify_domain(
        title=item.get("title"),
        slug=item.get("slug"),
        event_slug=item.get("eventSlug"),
    )


def _window_roi(closed_positions: list[dict[str, Any]], now: datetime, days: int, domain: str | None = None) -> float:
    cutoff = now - timedelta(days=days)
    pnl = 0.0
    bought = 0.0

    for item in closed_positions:
        ts = _parse_unix_ts(item.get("timestamp"))
        if ts is None or ts < cutoff:
            continue

        if domain is not None and _item_domain(item) != domain:
            continue

        pnl += _safe_float(item.get("realizedPnl"))
        bought += _safe_float(item.get("totalBought"))

    if bought <= 0:
        return 0.0

    return pnl / bought


def _monthly_rois(closed_positions: list[dict[str, Any]], now: datetime, months: int, only_negative: bool = False) -> list[float]:
    rois: list[float] = []

    for i in range(months):
        end = now - timedelta(days=30 * i)
        start = now - timedelta(days=30 * (i + 1))

        pnl = 0.0
        bought = 0.0

        for item in closed_positions:
            ts = _parse_unix_ts(item.get("timestamp"))
            if ts is None:
                continue
            if not (start <= ts < end):
                continue

            pnl += _safe_float(item.get("realizedPnl"))
            bought += _safe_float(item.get("totalBought"))

        roi = pnl / bought if bought > 0 else 0.0

        if only_negative:
            if roi < 0:
                rois.append(roi)
        else:
            rois.append(roi)

    return list(reversed(rois))


def _longest_loss_streak(closed_positions: list[dict[str, Any]]) -> int:
    items = sorted(closed_positions, key=lambda x: int(x.get("timestamp", 0) or 0))

    longest = 0
    current = 0

    for item in items:
        pnl = _safe_float(item.get("realizedPnl"))
        if pnl < 0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0

    return longest


def _profit_factor(closed_positions: list[dict[str, Any]]) -> float:
    gross_profit = 0.0
    gross_loss = 0.0

    for item in closed_positions:
        pnl = _safe_float(item.get("realizedPnl"))
        if pnl > 0:
            gross_profit += pnl
        elif pnl < 0:
            gross_loss += abs(pnl)

    if gross_loss == 0:
        return gross_profit if gross_profit > 0 else 0.0

    return gross_profit / gross_loss


def _largest_win_share(closed_positions: list[dict[str, Any]]) -> float:
    profits = [_safe_float(item.get("realizedPnl")) for item in closed_positions if _safe_float(item.get("realizedPnl")) > 0]
    gross_profit = sum(profits)

    if gross_profit <= 0 or not profits:
        return 1.0

    return max(profits) / gross_profit


def _max_drawdown_from_closed_positions(closed_positions: list[dict[str, Any]]) -> float:
    daily_returns: dict[str, float] = defaultdict(float)

    for item in closed_positions:
        ts = _parse_unix_ts(item.get("timestamp"))
        if ts is None:
            continue

        bought = _safe_float(item.get("totalBought"))
        pnl = _safe_float(item.get("realizedPnl"))
        if bought <= 0:
            continue

        day_key = ts.date().isoformat()
        daily_returns[day_key] += pnl / bought

    if not daily_returns:
        return 0.0

    equity = 1.0
    peak = 1.0
    max_dd = 0.0

    for day in sorted(daily_returns.keys()):
        equity *= (1.0 + daily_returns[day])
        peak = max(peak, equity)

        if peak > 0:
            dd = (peak - equity) / peak
            max_dd = max(max_dd, dd)

    return max_dd


def _primary_domain_stats(
    current_positions: list[dict[str, Any]],
    closed_positions: list[dict[str, Any]],
    trades: list[dict[str, Any]],
) -> tuple[str, float]:
    domain_counter: Counter[str] = Counter()
    all_items = list(current_positions) + list(closed_positions) + list(trades)

    for item in all_items:
        domain_counter[_item_domain(item)] += 1

    total = sum(domain_counter.values())
    if total == 0:
        return "other", 0.0

    primary_domain, count = domain_counter.most_common(1)[0]
    return primary_domain, count / total


def _single_market_concentration(
    current_positions: list[dict[str, Any]],
    closed_positions: list[dict[str, Any]],
    trades: list[dict[str, Any]],
) -> float:
    market_counter: Counter[str] = Counter()
    all_items = list(current_positions) + list(closed_positions) + list(trades)

    for item in all_items:
        key = _market_key(item)
        if key:
            market_counter[key] += 1

    total = sum(market_counter.values())
    if total == 0:
        return 1.0

    return market_counter.most_common(1)[0][1] / total


def _infer_unique_markets(
    traded_count: int,
    current_positions: list[dict[str, Any]],
    closed_positions: list[dict[str, Any]],
    trades: list[dict[str, Any]],
) -> int:
    unique_keys = set()

    for group in (current_positions, closed_positions, trades):
        for item in group:
            key = _market_key(item)
            if key:
                unique_keys.add(key)

    return max(traded_count, len(unique_keys))


def _activity_stats(
    trades: list[dict[str, Any]],
    now: datetime,
) -> tuple[int, int, int]:
    trade_times = [dt for item in trades if (dt := _item_dt(item)) is not None]
    if not trade_times:
        return 0, 0, 9999

    cutoff_30 = now - timedelta(days=30)
    cutoff_90 = now - timedelta(days=90)
    trades_30d = sum(1 for dt in trade_times if dt >= cutoff_30)
    trades_90d = sum(1 for dt in trade_times if dt >= cutoff_90)
    days_since_last_trade = max((now - max(trade_times)).days, 0)
    return trades_30d, trades_90d, days_since_last_trade


def build_wallet_metrics(
    profile: dict[str, Any],
    traded_count: int,
    current_positions: list[dict[str, Any]],
    closed_positions: list[dict[str, Any]],
    trades: list[dict[str, Any]],
    median_spread: float = 0.015,
    median_liquidity: float = 10000.0,
    slippage_proxy: float = 0.01,
    delay_sec: float = 60.0,
) -> WalletMetrics:
    now = datetime.now(timezone.utc)

    created_at = _parse_iso_dt(profile.get("createdAt"))
    age_days = (now - created_at).days if created_at else 0

    primary_domain, primary_domain_share = _primary_domain_stats(
        current_positions=current_positions,
        closed_positions=closed_positions,
        trades=trades,
    )
    trades_30d, trades_90d, days_since_last_trade = _activity_stats(trades, now)

    return WalletMetrics(
        age_days=age_days,
        closed_positions=len(closed_positions),
        unique_markets=_infer_unique_markets(
            traded_count=traded_count,
            current_positions=current_positions,
            closed_positions=closed_positions,
            trades=trades,
        ),
        primary_domain_share=primary_domain_share,
        single_market_concentration=_single_market_concentration(
            current_positions=current_positions,
            closed_positions=closed_positions,
            trades=trades,
        ),
        roi_30=_window_roi(closed_positions, now, 30),
        roi_90=_window_roi(closed_positions, now, 90),
        roi_180=_window_roi(closed_positions, now, 180),
        monthly_roi_last_6=_monthly_rois(closed_positions, now, 6, only_negative=False),
        negative_monthly_roi_last_12=_monthly_rois(closed_positions, now, 12, only_negative=True),
        primary_domain_roi_30=_window_roi(closed_positions, now, 30, domain=primary_domain),
        primary_domain_roi_90=_window_roi(closed_positions, now, 90, domain=primary_domain),
        primary_domain_roi_180=_window_roi(closed_positions, now, 180, domain=primary_domain),
        max_drawdown=_max_drawdown_from_closed_positions(closed_positions),
        longest_loss_streak=_longest_loss_streak(closed_positions),
        median_spread=median_spread,
        median_liquidity=median_liquidity,
        slippage_proxy=slippage_proxy,
        delay_sec=delay_sec,
        profit_factor=_profit_factor(closed_positions),
        largest_win_share=_largest_win_share(closed_positions),
        trades_30d=trades_30d,
        trades_90d=trades_90d,
        days_since_last_trade=days_since_last_trade,
    )


def wallet_metrics_to_dict(metrics: WalletMetrics) -> dict[str, Any]:
    return asdict(metrics)
