from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from collectors.wallet_profiles import WalletProfilesClient
from execution.builder_auth import load_executor_config
from execution.copy_worker import LeaderSignal
from execution.order_policy import evaluate_order_policy
from execution.polymarket_executor import fetch_market_snapshot
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


def _safe_float(value: Any) -> float:
    try:
        return float(value) if value is not None else 0.0
    except Exception:
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(value) if value is not None else 0
    except Exception:
        return 0


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

    abs_drift = abs(current_price - leader_price)
    rel_drift = abs_drift / leader_price

    if side == "BUY":
        if current_price > leader_price:
            if abs_drift > max_abs:
                return False, f"buy price drift abs too high: {abs_drift:.4f} > {max_abs:.4f}"
            if rel_drift > max_rel:
                return False, f"buy price drift rel too high: {rel_drift:.4f} > {max_rel:.4f}"
    elif side == "SELL":
        if current_price < leader_price:
            if abs_drift > max_abs:
                return False, f"sell price drift abs too high: {abs_drift:.4f} > {max_abs:.4f}"
            if rel_drift > max_rel:
                return False, f"sell price drift rel too high: {rel_drift:.4f} > {max_rel:.4f}"
    else:
        return False, f"unsupported side: {side}"

    return True, "ok"


def latest_fresh_copyable_signal_from_wallet(
    wallet: str,
    leader_budget_usd: float,
) -> tuple[LeaderSignal | None, dict | None, dict]:
    client = WalletProfilesClient()
    config = load_executor_config()

    risk = config.get("risk", {})
    filters = config.get("filters", {})
    freshness = config.get("signal_freshness", {})
    exit_cfg = config.get("exit", {})

    preferred_signal_age_sec = int(freshness.get("preferred_signal_age_sec", 30))
    max_recent_trades = int(freshness.get("max_recent_trades", 3))
    max_price_drift_abs = float(freshness.get("max_price_drift_abs", 0.01))
    max_price_drift_rel = float(freshness.get("max_price_drift_rel", 0.02))

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

    normalized = [normalize_trade(x) for x in trades]
    normalized.sort(key=lambda x: x.timestamp, reverse=True)

    now_ts = int(time.time())

    summary = {
        "wallet": wallet,
        "leader_status": leader_status,
        "checked_trades": len(normalized),
        "latest_trade_side": None,
        "latest_trade_age_sec": None,
        "latest_trade_hash": None,
        "latest_status": None,
        "latest_reason": None,
        "selected_trade_hash": None,
        "selected_trade_age_sec": None,
        "selected_status": None,
        "selected_reason": None,
        "selected_has_open_position": None,
    }

    for idx, trade in enumerate(normalized):
        age_sec = now_ts - trade.timestamp
        open_position = get_open_position(wallet, trade.asset) if trade.asset else None
        has_open_position = open_position is not None

        if idx == 0:
            summary["latest_trade_side"] = trade.side
            summary["latest_trade_age_sec"] = age_sec
            summary["latest_trade_hash"] = trade.transaction_hash

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

        if has_signal(trade.transaction_hash):
            if idx == 0:
                summary["latest_status"] = "ALREADY_PROCESSED"
                summary["latest_reason"] = "already processed"
            continue

        if leader_status == "EXIT_ONLY" and trade.side == "BUY":
            if idx == 0:
                summary["latest_status"] = "EXIT_ONLY_BUY_BLOCKED"
                summary["latest_reason"] = "leader is EXIT_ONLY; new buys blocked"
            continue

        if trade.side == "SELL" and not has_open_position:
            if idx == 0:
                summary["latest_status"] = "SKIPPED_NO_POSITION"
                summary["latest_reason"] = "sell signal but no copied open position"
            continue

        snapshot = fetch_market_snapshot(token_id=trade.asset, side=trade.side)

        max_spread = float(risk.get("skip_if_spread_gt", 0.02))
        if trade.side == "SELL" and has_open_position:
            max_spread = exit_max_spread

        policy = evaluate_order_policy(
            side=trade.side,
            midpoint=snapshot["midpoint"],
            spread=snapshot["spread"],
            leader_budget_usd=leader_budget_usd,
            buy_min_price=float(filters.get("buy_min_price", 0.05)),
            buy_max_price=float(filters.get("buy_max_price", 0.95)),
            sell_min_price=0.0,
            sell_max_price=1.0,
            max_spread=max_spread,
            min_order_size_usd=float(risk.get("min_order_size_usd", 1.0)),
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
            selected_status = "EXIT_FOLLOW_STALE" if age_sec > preferred_signal_age_sec else "EXIT_FOLLOW"
        else:
            selected_status = "FRESH_COPYABLE" if age_sec <= preferred_signal_age_sec else "LATE_BUT_COPYABLE"

        if idx == 0:
            summary["latest_status"] = selected_status
            summary["latest_reason"] = "copyable"

        summary["selected_trade_hash"] = trade.transaction_hash
        summary["selected_trade_age_sec"] = age_sec
        summary["selected_status"] = selected_status
        summary["selected_reason"] = "copyable"
        summary["selected_has_open_position"] = has_open_position

        signal = LeaderSignal(
            signal_id=trade.transaction_hash,
            leader_wallet=wallet,
            token_id=trade.asset,
            side=trade.side,
            leader_budget_usd=leader_budget_usd,
        )

        return signal, snapshot, summary

    if summary["latest_status"] is None:
        summary["latest_status"] = "NO_USABLE_RECENT_TRADES"
        summary["latest_reason"] = "no usable recent trades"

    return None, None, summary
