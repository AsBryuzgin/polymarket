from __future__ import annotations

from dataclasses import dataclass, asdict

from execution.builder_auth import load_executor_config
from execution.copy_sizer import compute_copy_size
from execution.order_policy import evaluate_order_policy
from execution.polymarket_executor import fetch_market_snapshot, preview_market_order
from execution.state_store import has_signal, record_signal


@dataclass
class LeaderSignal:
    signal_id: str
    leader_wallet: str
    token_id: str
    side: str
    leader_budget_usd: float


def process_signal(signal: LeaderSignal) -> dict:
    if has_signal(signal.signal_id):
        return {
            "signal_id": signal.signal_id,
            "status": "DUPLICATE",
            "reason": "signal already processed",
        }

    config = load_executor_config()
    risk = config.get("risk", {})
    filters = config.get("filters", {})
    sizing = config.get("sizing", {})

    snapshot = fetch_market_snapshot(token_id=signal.token_id, side=signal.side)

    policy = evaluate_order_policy(
        side=signal.side,
        midpoint=snapshot["midpoint"],
        spread=snapshot["spread"],
        leader_budget_usd=signal.leader_budget_usd,
        buy_min_price=float(filters.get("buy_min_price", 0.05)),
        buy_max_price=float(filters.get("buy_max_price", 0.95)),
        sell_min_price=0.0,
        sell_max_price=1.0,
        max_spread=float(risk.get("skip_if_spread_gt", 0.02)),
        min_order_size_usd=float(risk.get("min_order_size_usd", 1.0)),
    )

    if not policy.allowed:
        record_signal(
            signal_id=signal.signal_id,
            leader_wallet=signal.leader_wallet,
            token_id=signal.token_id,
            side=signal.side,
            leader_budget_usd=signal.leader_budget_usd,
            suggested_amount_usd=None,
            status="SKIPPED_POLICY",
            reason=policy.reason,
        )
        return {
            "signal": asdict(signal),
            "market_snapshot": snapshot,
            "status": "SKIPPED_POLICY",
            "reason": policy.reason,
        }

    size = compute_copy_size(
        leader_budget_usd=signal.leader_budget_usd,
        target_trade_fraction=float(sizing.get("target_trade_fraction", 0.20)),
        min_order_size_usd=float(risk.get("min_order_size_usd", 1.0)),
        max_per_trade_usd=float(risk.get("max_per_trade_usd", 2.0)),
    )

    if not size.allowed:
        record_signal(
            signal_id=signal.signal_id,
            leader_wallet=signal.leader_wallet,
            token_id=signal.token_id,
            side=signal.side,
            leader_budget_usd=signal.leader_budget_usd,
            suggested_amount_usd=None,
            status="SKIPPED_SIZING",
            reason=size.reason,
        )
        return {
            "signal": asdict(signal),
            "market_snapshot": snapshot,
            "status": "SKIPPED_SIZING",
            "reason": size.reason,
        }

    preview = preview_market_order(
        token_id=signal.token_id,
        amount_usd=size.amount_usd,
        side=signal.side,
    )

    record_signal(
        signal_id=signal.signal_id,
        leader_wallet=signal.leader_wallet,
        token_id=signal.token_id,
        side=signal.side,
        leader_budget_usd=signal.leader_budget_usd,
        suggested_amount_usd=size.amount_usd,
        status="PREVIEW_READY",
        reason="ok",
    )

    return {
        "signal": asdict(signal),
        "market_snapshot": snapshot,
        "status": "PREVIEW_READY",
        "reason": "ok",
        "suggested_amount_usd": size.amount_usd,
        "preview_order": preview,
    }
