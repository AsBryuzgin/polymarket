from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime

from execution.builder_auth import load_executor_config
from execution.copy_sizer import compute_copy_size
from execution.order_policy import evaluate_order_policy
from execution.polymarket_executor import fetch_market_snapshot, preview_market_order
from execution.state_store import (
    has_signal,
    record_signal,
    get_open_position,
    upsert_buy_position,
    close_position,
    get_leader_registry,
    log_trade_event,
)


@dataclass
class LeaderSignal:
    signal_id: str
    leader_wallet: str
    token_id: str
    side: str
    leader_budget_usd: float


def _parse_opened_at_to_minutes(opened_at: str | None) -> float | None:
    if not opened_at:
        return None
    try:
        dt = datetime.fromisoformat(opened_at.replace(" ", "T"))
        now = datetime.utcnow()
        return round((now - dt).total_seconds() / 60.0, 2)
    except Exception:
        return None


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
    exit_cfg = config.get("exit", {})

    registry = get_leader_registry(signal.leader_wallet)
    leader_user_name = registry["user_name"] if registry else None
    category = registry["category"] if registry else None
    leader_status = registry["leader_status"] if registry else None

    snapshot = fetch_market_snapshot(token_id=signal.token_id, side=signal.side)
    current_price = snapshot["price_quote"]

    if signal.side.upper() == "SELL":
        open_position = get_open_position(signal.leader_wallet, signal.token_id)

        if open_position is None:
            record_signal(
                signal_id=signal.signal_id,
                leader_wallet=signal.leader_wallet,
                token_id=signal.token_id,
                side=signal.side,
                leader_budget_usd=signal.leader_budget_usd,
                suggested_amount_usd=None,
                status="SKIPPED_NO_POSITION",
                reason="sell signal but no copied open position",
            )
            return {
                "signal": asdict(signal),
                "market_snapshot": snapshot,
                "status": "SKIPPED_NO_POSITION",
                "reason": "sell signal but no copied open position",
            }

        policy = evaluate_order_policy(
            side=signal.side,
            midpoint=snapshot["midpoint"],
            spread=snapshot["spread"],
            leader_budget_usd=signal.leader_budget_usd,
            buy_min_price=float(filters.get("buy_min_price", 0.05)),
            buy_max_price=float(filters.get("buy_max_price", 0.95)),
            sell_min_price=0.0,
            sell_max_price=1.0,
            max_spread=float(exit_cfg.get("exit_max_spread", 0.05)),
            min_order_size_usd=0.0,
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

        position_usd = float(open_position["position_usd"])
        if position_usd <= 0:
            record_signal(
                signal_id=signal.signal_id,
                leader_wallet=signal.leader_wallet,
                token_id=signal.token_id,
                side=signal.side,
                leader_budget_usd=signal.leader_budget_usd,
                suggested_amount_usd=None,
                status="SKIPPED_NO_POSITION",
                reason="open position has zero size",
            )
            return {
                "signal": asdict(signal),
                "market_snapshot": snapshot,
                "status": "SKIPPED_NO_POSITION",
                "reason": "open position has zero size",
            }

        preview = preview_market_order(
            token_id=signal.token_id,
            amount_usd=round(position_usd, 2),
            side=signal.side,
        )

        closed = close_position(
            leader_wallet=signal.leader_wallet,
            token_id=signal.token_id,
            signal_id=signal.signal_id,
        )

        entry_avg_price = closed["entry_avg_price"] if closed else None
        position_before_usd = closed["position_before_usd"] if closed else position_usd
        position_after_usd = closed["position_after_usd"] if closed else 0.0
        holding_minutes = _parse_opened_at_to_minutes(closed["opened_at"] if closed else None)

        realized_pnl_usd = None
        realized_pnl_pct = None

        if entry_avg_price is not None and current_price is not None:
            realized_pnl_pct = round((float(current_price) - float(entry_avg_price)) / float(entry_avg_price), 6)
            realized_pnl_usd = round(position_before_usd * realized_pnl_pct, 4)

        log_trade_event(
            signal_id=signal.signal_id,
            leader_wallet=signal.leader_wallet,
            leader_user_name=leader_user_name,
            category=category,
            leader_status=leader_status,
            token_id=signal.token_id,
            side=signal.side,
            event_type="EXIT",
            amount_usd=round(position_before_usd, 2),
            price=current_price,
            gross_value_usd=round(position_before_usd, 2),
            position_before_usd=position_before_usd,
            position_after_usd=position_after_usd,
            entry_avg_price=entry_avg_price,
            exit_price=current_price,
            realized_pnl_usd=realized_pnl_usd,
            realized_pnl_pct=realized_pnl_pct,
            holding_minutes=holding_minutes,
            notes="preview exit generated",
        )

        record_signal(
            signal_id=signal.signal_id,
            leader_wallet=signal.leader_wallet,
            token_id=signal.token_id,
            side=signal.side,
            leader_budget_usd=signal.leader_budget_usd,
            suggested_amount_usd=round(position_usd, 2),
            status="PREVIEW_READY_EXIT",
            reason="ok",
        )

        return {
            "signal": asdict(signal),
            "market_snapshot": snapshot,
            "status": "PREVIEW_READY_EXIT",
            "reason": "ok",
            "suggested_amount_usd": round(position_usd, 2),
            "preview_order": preview,
            "realized_pnl_usd": realized_pnl_usd,
            "realized_pnl_pct": realized_pnl_pct,
            "holding_minutes": holding_minutes,
        }

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

    pos_update = upsert_buy_position(
        leader_wallet=signal.leader_wallet,
        token_id=signal.token_id,
        amount_usd=size.amount_usd,
        entry_price=current_price,
        signal_id=signal.signal_id,
    )

    log_trade_event(
        signal_id=signal.signal_id,
        leader_wallet=signal.leader_wallet,
        leader_user_name=leader_user_name,
        category=category,
        leader_status=leader_status,
        token_id=signal.token_id,
        side=signal.side,
        event_type="ENTRY",
        amount_usd=size.amount_usd,
        price=current_price,
        gross_value_usd=size.amount_usd,
        position_before_usd=pos_update["position_before_usd"],
        position_after_usd=pos_update["position_after_usd"],
        entry_avg_price=pos_update["entry_avg_price_after"],
        exit_price=None,
        realized_pnl_usd=None,
        realized_pnl_pct=None,
        holding_minutes=None,
        notes="preview entry generated",
    )

    record_signal(
        signal_id=signal.signal_id,
        leader_wallet=signal.leader_wallet,
        token_id=signal.token_id,
        side=signal.side,
        leader_budget_usd=signal.leader_budget_usd,
        suggested_amount_usd=size.amount_usd,
        status="PREVIEW_READY_ENTRY",
        reason="ok",
    )

    return {
        "signal": asdict(signal),
        "market_snapshot": snapshot,
        "status": "PREVIEW_READY_ENTRY",
        "reason": "ok",
        "suggested_amount_usd": size.amount_usd,
        "preview_order": preview,
    }
