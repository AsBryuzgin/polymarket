from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from collectors.wallet_profiles import WalletProfilesClient
from execution.builder_auth import load_executor_config
from execution.copy_worker import LeaderSignal
from execution.order_policy import evaluate_order_policy
from execution.price_drift import price_drift_ok
from execution.polymarket_executor import fetch_market_snapshot
from execution.onchain_shadow import (
    list_recent_onchain_shadow_trades,
    onchain_signal_id,
    record_data_api_trade_seen,
)
from execution.state_store import has_signal, get_open_position, get_leader_registry


@dataclass
class RawLeaderTrade:
    proxy_wallet: str
    side: str
    asset: str
    condition_id: str
    size: float
    price: float
    timestamp: int
    title: str
    slug: str
    event_slug: str
    outcome: str
    transaction_hash: str
    signal_id: str = ""
    source: str = "data_api"


@dataclass
class CopyableSignalCandidate:
    signal: LeaderSignal
    snapshot: dict[str, Any]
    summary: dict[str, Any]


def _safe_float(value: Any) -> float:
    try:
        return float(value) if value is not None else 0.0
    except Exception:
        return 0.0


def _positive_float_or_none(value: Any) -> float | None:
    parsed = _safe_float(value)
    return parsed if parsed > 0 else None


def _safe_int(value: Any) -> int:
    try:
        return int(value) if value is not None else 0
    except Exception:
        return 0


def _position_asset(item: dict[str, Any]) -> str:
    for key in ("asset", "assetId", "token_id", "tokenId"):
        value = item.get(key)
        if value:
            return str(value)
    return ""


def _load_leader_positions(
    *,
    client: WalletProfilesClient,
    wallet: str,
    max_pages: int,
) -> tuple[list[dict[str, Any]], str | None]:
    try:
        return client.paginate_current_positions(
            user=wallet,
            page_size=100,
            max_pages=max_pages,
        ), None
    except Exception as e:
        return [], str(e)


def _leader_position_context_from_positions(
    *,
    positions: list[dict[str, Any]],
    position_error: str | None,
    token_id: str,
    side: str,
    trade_size: float,
) -> dict[str, Any]:
    if position_error:
        return {
            "leader_portfolio_value_usd": None,
            "leader_token_position_size": None,
            "leader_token_position_value_usd": None,
            "leader_exit_fraction": None,
            "leader_position_context_error": position_error,
        }
    portfolio_value = 0.0
    token_position_size = 0.0
    token_position_value = 0.0

    for item in positions:
        portfolio_value += _safe_float(item.get("currentValue"))
        if _position_asset(item) == token_id:
            token_position_size += _safe_float(item.get("size"))
            token_position_value += _safe_float(item.get("currentValue"))

    exit_fraction = None
    if side == "SELL" and trade_size > 0:
        pre_trade_size = token_position_size + trade_size
        if pre_trade_size > 0:
            exit_fraction = min(1.0, trade_size / pre_trade_size)

    return {
        "leader_portfolio_value_usd": _positive_float_or_none(portfolio_value),
        "leader_token_position_size": _positive_float_or_none(token_position_size),
        "leader_token_position_value_usd": _positive_float_or_none(token_position_value),
        "leader_exit_fraction": exit_fraction,
        "leader_position_context_error": None,
    }


def _leader_position_context(
    *,
    client: WalletProfilesClient,
    wallet: str,
    token_id: str,
    side: str,
    trade_size: float,
    max_pages: int,
) -> dict[str, Any]:
    positions, error = _load_leader_positions(
        client=client,
        wallet=wallet,
        max_pages=max_pages,
    )
    return _leader_position_context_from_positions(
        positions=positions,
        position_error=error,
        token_id=token_id,
        side=side,
        trade_size=trade_size,
    )


def normalize_trade(item: dict[str, Any]) -> RawLeaderTrade:
    return RawLeaderTrade(
        proxy_wallet=str(item.get("proxyWallet") or ""),
        side=str(item.get("side") or "").upper(),
        asset=str(item.get("asset") or ""),
        condition_id=str(item.get("conditionId") or ""),
        size=_safe_float(item.get("size")),
        price=_safe_float(item.get("price")),
        timestamp=_safe_int(item.get("timestamp")),
        title=str(item.get("title") or ""),
        slug=str(item.get("slug") or ""),
        event_slug=str(item.get("eventSlug") or ""),
        outcome=str(item.get("outcome") or ""),
        transaction_hash=str(item.get("transactionHash") or ""),
        signal_id=str(item.get("signalId") or item.get("signal_id") or ""),
        source=str(item.get("source") or "data_api"),
    )


def _price_drift_ok(
    leader_price: float,
    current_price: float,
    side: str,
    max_abs: float,
    max_rel: float,
) -> tuple[bool, str]:
    if leader_price <= 0 or current_price <= 0:
        return False, "invalid leader/current price"
    return price_drift_ok(
        leader_price=leader_price,
        current_price=current_price,
        side=side,
        max_abs=max_abs,
        max_rel=max_rel,
    )


def _base_summary(*, wallet: str, leader_status: str, checked_trades: int) -> dict[str, Any]:
    return {
        "wallet": wallet,
        "leader_status": leader_status,
        "checked_trades": checked_trades,
        "latest_trade_side": None,
        "latest_trade_age_sec": None,
        "latest_trade_hash": None,
        "latest_token_id": None,
        "latest_trade_price": None,
        "latest_snapshot_midpoint": None,
        "latest_snapshot_best_bid": None,
        "latest_snapshot_best_ask": None,
        "latest_snapshot_spread": None,
        "latest_status": None,
        "latest_reason": None,
        "selected_trade_hash": None,
        "selected_trade_age_sec": None,
        "selected_status": None,
        "selected_reason": None,
        "selected_has_open_position": None,
        "selected_trade_notional_usd": None,
        "selected_leader_portfolio_value_usd": None,
        "selected_leader_token_position_size": None,
        "selected_leader_token_position_value_usd": None,
        "selected_leader_exit_fraction": None,
        "selected_leader_position_context_error": None,
    }


def _effective_signal_id(trade: RawLeaderTrade) -> str:
    return trade.signal_id or trade.transaction_hash


def _trade_fingerprint(trade: RawLeaderTrade) -> str:
    if trade.transaction_hash and trade.asset and trade.side:
        return f"{trade.transaction_hash.lower()}:{trade.asset}:{trade.side.upper()}"
    return _effective_signal_id(trade) or (
        f"{trade.source}:{trade.asset}:{trade.side}:{trade.timestamp}:{trade.size}:{trade.price}"
    )


def _signal_aliases(trade: RawLeaderTrade) -> list[str]:
    aliases: list[str] = []
    effective = _effective_signal_id(trade)
    if effective:
        aliases.append(effective)
    if trade.transaction_hash:
        aliases.append(trade.transaction_hash)
    if trade.transaction_hash and trade.asset and trade.side:
        aliases.append(
            onchain_signal_id(
                transaction_hash=trade.transaction_hash,
                token_id=trade.asset,
                side=trade.side,
            )
        )
    deduped: list[str] = []
    seen: set[str] = set()
    for alias in aliases:
        if alias and alias not in seen:
            deduped.append(alias)
            seen.add(alias)
    return deduped


def _has_processed_trade(trade: RawLeaderTrade) -> bool:
    return any(has_signal(alias) for alias in _signal_aliases(trade))


def _dedupe_and_sort_trades(trades: list[dict[str, Any]]) -> list[RawLeaderTrade]:
    normalized: list[RawLeaderTrade] = []
    seen: set[str] = set()
    for item in trades:
        trade = normalize_trade(item)
        key = _trade_fingerprint(trade)
        if key in seen:
            continue
        seen.add(key)
        normalized.append(trade)
    normalized.sort(key=lambda x: x.timestamp, reverse=True)
    return normalized


def _merge_trade_sources(
    data_api_trades: list[RawLeaderTrade],
    onchain_trades: list[RawLeaderTrade],
) -> list[RawLeaderTrade]:
    by_fingerprint: dict[str, RawLeaderTrade] = {}
    for trade in data_api_trades:
        by_fingerprint[_trade_fingerprint(trade)] = trade
    for trade in onchain_trades:
        by_fingerprint[_trade_fingerprint(trade)] = trade
    merged = list(by_fingerprint.values())
    merged.sort(key=lambda x: x.timestamp, reverse=True)
    return merged


def fresh_copyable_signals_from_wallet(
    wallet: str,
    leader_budget_usd: float,
) -> tuple[list[CopyableSignalCandidate], dict]:
    client = WalletProfilesClient()
    config = load_executor_config()

    risk = config.get("risk", {})
    filters = config.get("filters", {})
    freshness = config.get("signal_freshness", {})
    exit_cfg = config.get("exit", {})

    preferred_signal_age_sec = int(freshness.get("preferred_signal_age_sec", 30))
    max_buy_signal_age_sec = int(
        freshness.get(
            "max_buy_signal_age_sec",
            freshness.get("max_copyable_signal_age_sec", 600),
        )
    )
    max_exit_signal_age_sec = int(freshness.get("max_exit_signal_age_sec", 86400))
    max_recent_trades = int(freshness.get("max_recent_trades", 3))
    max_signals_per_cycle = max(1, int(freshness.get("max_signals_per_cycle", 1)))
    max_price_drift_abs = float(freshness.get("max_price_drift_abs", 0.02))
    max_price_drift_rel = float(freshness.get("max_price_drift_rel", 0.03))
    max_position_pages = int(freshness.get("leader_position_context_max_pages", 20))

    ignore_exit_drift = bool(exit_cfg.get("ignore_exit_drift", True))
    exit_max_spread = float(exit_cfg.get("exit_max_spread", 0.05))

    leader_registry = get_leader_registry(wallet)
    leader_status = leader_registry["leader_status"] if leader_registry else "ACTIVE"

    trades = client.get_trades(
        user=wallet,
        limit=max_recent_trades,
        offset=0,
        taker_only=True,
    )

    data_api_trades = _dedupe_and_sort_trades(trades)
    if bool(config.get("onchain_shadow", {}).get("enabled", False)):
        for trade in data_api_trades:
            record_data_api_trade_seen(
                transaction_hash=trade.transaction_hash,
                leader_wallet=wallet,
                token_id=trade.asset,
                side=trade.side,
                trade_timestamp=trade.timestamp,
            )

    onchain_trades: list[RawLeaderTrade] = []
    onchain_cfg = config.get("onchain_shadow", {})
    if bool(onchain_cfg.get("enabled", False)) and bool(
        onchain_cfg.get("use_as_signal_source", False)
    ):
        onchain_limit = int(onchain_cfg.get("signal_limit", max_recent_trades))
        onchain_window_sec = int(onchain_cfg.get("signal_window_sec", max_buy_signal_age_sec))
        onchain_trades = _dedupe_and_sort_trades(
            list_recent_onchain_shadow_trades(
                leader_wallet=wallet,
                limit=max(onchain_limit, max_recent_trades),
                max_age_sec=max(onchain_window_sec, max_buy_signal_age_sec),
            )
        )

    normalized = _merge_trade_sources(data_api_trades, onchain_trades)
    now_ts = int(time.time())
    summary = _base_summary(
        wallet=wallet,
        leader_status=leader_status,
        checked_trades=len(normalized),
    )
    candidates: list[CopyableSignalCandidate] = []
    position_cache: tuple[list[dict[str, Any]], str | None] | None = None

    for idx, trade in enumerate(normalized):
        age_sec = now_ts - trade.timestamp
        open_position = get_open_position(wallet, trade.asset) if trade.asset else None
        has_open_position = open_position is not None
        newer_same_token_sell = any(
            newer.asset == trade.asset and newer.side == "SELL"
            for newer in normalized[:idx]
        )

        if idx == 0:
            summary["latest_trade_side"] = trade.side
            summary["latest_trade_age_sec"] = age_sec
            summary["latest_trade_hash"] = _effective_signal_id(trade)
            summary["latest_token_id"] = trade.asset
            summary["latest_trade_price"] = trade.price

        if trade.side not in {"BUY", "SELL"}:
            if idx == 0:
                summary["latest_status"] = "UNSUPPORTED_SIDE"
                summary["latest_reason"] = f"unsupported side: {trade.side}"
            continue

        if not trade.asset or not trade.transaction_hash:
            if idx == 0:
                summary["latest_status"] = "MALFORMED_TRADE"
                summary["latest_reason"] = "missing asset or transaction_hash"
            continue

        if _has_processed_trade(trade):
            if idx == 0:
                summary["latest_status"] = "ALREADY_PROCESSED"
                summary["latest_reason"] = "already processed"
            continue

        if leader_status == "EXIT_ONLY" and trade.side == "BUY":
            if idx == 0:
                summary["latest_status"] = "EXIT_ONLY_BUY_BLOCKED"
                summary["latest_reason"] = "leader is EXIT_ONLY; new buys blocked"
            continue

        if trade.side == "BUY" and newer_same_token_sell:
            if idx == 0:
                summary["latest_status"] = "SUPERSEDED_BY_NEWER_SELL"
                summary["latest_reason"] = "newer sell for the same token is already visible"
            continue

        max_signal_age_sec = (
            max_buy_signal_age_sec if trade.side == "BUY" else max_exit_signal_age_sec
        )
        if max_signal_age_sec > 0 and age_sec > max_signal_age_sec:
            if idx == 0:
                summary["latest_status"] = "TOO_OLD"
                summary["latest_reason"] = (
                    f"{trade.side.lower()} signal age {age_sec}s above "
                    f"max_signal_age_sec {max_signal_age_sec}s"
                )
            continue

        if trade.side == "SELL" and not has_open_position:
            if idx == 0:
                summary["latest_status"] = "SKIPPED_NO_POSITION"
                summary["latest_reason"] = "sell signal but no copied open position"
            continue

        try:
            snapshot = fetch_market_snapshot(token_id=trade.asset, side=trade.side)
        except Exception as e:
            msg = str(e)
            if idx == 0:
                if "No orderbook exists" in msg or "404" in msg:
                    summary["latest_status"] = "NO_ORDERBOOK"
                    summary["latest_reason"] = "no orderbook for token"
                else:
                    summary["latest_status"] = "SNAPSHOT_ERROR"
                    summary["latest_reason"] = msg
            continue

        if idx == 0:
            summary["latest_snapshot_midpoint"] = snapshot.get("midpoint")
            summary["latest_snapshot_best_bid"] = snapshot.get("best_bid")
            summary["latest_snapshot_best_ask"] = snapshot.get("best_ask")
            summary["latest_snapshot_spread"] = snapshot.get("spread")

        max_spread = float(risk.get("skip_if_spread_gt", 0.02))
        max_spread_rel = _positive_float_or_none(risk.get("skip_if_spread_rel_gt"))
        max_spread_hard = _positive_float_or_none(risk.get("skip_if_spread_hard_gt"))
        if trade.side == "SELL" and has_open_position:
            max_spread = exit_max_spread
            max_spread_rel = _positive_float_or_none(exit_cfg.get("exit_max_spread_rel"))
            max_spread_hard = _positive_float_or_none(exit_cfg.get("exit_max_spread_hard"))

        policy = evaluate_order_policy(
            side=trade.side,
            midpoint=snapshot["midpoint"],
            spread=snapshot["spread"],
            leader_budget_usd=leader_budget_usd,
            buy_min_price=float(filters.get("buy_min_price", 0.05)),
            buy_max_price=float(filters.get("buy_max_price", 0.96)),
            sell_min_price=0.0,
            sell_max_price=1.0,
            max_spread=max_spread,
            min_order_size_usd=float(risk.get("min_order_size_usd", 1.0)),
            max_spread_rel=max_spread_rel,
            max_spread_hard=max_spread_hard,
        )

        if not policy.allowed:
            if idx == 0:
                summary["latest_status"] = "POLICY_BLOCKED"
                summary["latest_reason"] = policy.reason
            continue

        current_price = snapshot["price_quote"]
        if current_price is None:
            if idx == 0:
                summary["latest_status"] = "NO_PRICE_QUOTE"
                summary["latest_reason"] = "missing current price quote"
            continue

        drift_ok = True
        drift_reason = "ok"

        if not (trade.side == "SELL" and has_open_position and ignore_exit_drift):
            drift_ok, drift_reason = _price_drift_ok(
                leader_price=trade.price,
                current_price=float(current_price),
                side=trade.side,
                max_abs=max_price_drift_abs,
                max_rel=max_price_drift_rel,
            )

        if not drift_ok:
            if idx == 0:
                summary["latest_status"] = "DRIFT_BLOCKED"
                summary["latest_reason"] = drift_reason
            continue

        if trade.side == "SELL" and has_open_position:
            selected_status = "EXIT_FOLLOW" if age_sec <= preferred_signal_age_sec else "EXIT_FOLLOW_STALE"
        else:
            selected_status = "FRESH_COPYABLE" if age_sec <= preferred_signal_age_sec else "LATE_BUT_COPYABLE"

        trade_notional_usd = None
        if trade.size > 0 and trade.price > 0:
            trade_notional_usd = trade.size * trade.price

        if idx == 0:
            summary["latest_status"] = selected_status
            summary["latest_reason"] = "copyable"

        if position_cache is None:
            position_cache = _load_leader_positions(
                client=client,
                wallet=wallet,
                max_pages=max_position_pages,
            )
        positions, position_error = position_cache
        position_context = _leader_position_context_from_positions(
            positions=positions,
            position_error=position_error,
            token_id=trade.asset,
            side=trade.side,
            trade_size=trade.size,
        )

        selected_summary = dict(summary)
        selected_summary["selected_trade_hash"] = _effective_signal_id(trade)
        selected_summary["selected_trade_age_sec"] = age_sec
        selected_summary["selected_status"] = selected_status
        selected_summary["selected_reason"] = "copyable"
        selected_summary["selected_has_open_position"] = has_open_position
        selected_summary["selected_trade_notional_usd"] = trade_notional_usd
        selected_summary["selected_leader_portfolio_value_usd"] = position_context[
            "leader_portfolio_value_usd"
        ]
        selected_summary["selected_leader_token_position_size"] = position_context[
            "leader_token_position_size"
        ]
        selected_summary["selected_leader_token_position_value_usd"] = position_context[
            "leader_token_position_value_usd"
        ]
        selected_summary["selected_leader_exit_fraction"] = position_context[
            "leader_exit_fraction"
        ]
        selected_summary["selected_leader_position_context_error"] = position_context[
            "leader_position_context_error"
        ]

        signal = LeaderSignal(
            signal_id=_effective_signal_id(trade),
            leader_wallet=wallet,
            token_id=trade.asset,
            side=trade.side,
            leader_budget_usd=leader_budget_usd,
            leader_trade_size=trade.size,
            leader_trade_price=trade.price,
            leader_trade_notional_usd=trade_notional_usd,
            leader_portfolio_value_usd=position_context["leader_portfolio_value_usd"],
            leader_token_position_size=position_context["leader_token_position_size"],
            leader_token_position_value_usd=position_context["leader_token_position_value_usd"],
            leader_exit_fraction=position_context["leader_exit_fraction"],
        )

        candidates.append(
            CopyableSignalCandidate(
                signal=signal,
                snapshot=snapshot,
                summary=selected_summary,
            )
        )
        if len(candidates) >= max_signals_per_cycle:
            break

    if summary["latest_status"] is None:
        summary["latest_status"] = "NO_USABLE_RECENT_TRADES"
        summary["latest_reason"] = "no usable recent trades"

    return candidates, summary


def latest_fresh_copyable_signal_from_wallet(
    wallet: str,
    leader_budget_usd: float,
) -> tuple[LeaderSignal | None, dict | None, dict]:
    candidates, summary = fresh_copyable_signals_from_wallet(
        wallet=wallet,
        leader_budget_usd=leader_budget_usd,
    )
    if not candidates:
        return None, None, summary
    candidate = candidates[0]
    return candidate.signal, candidate.snapshot, candidate.summary
