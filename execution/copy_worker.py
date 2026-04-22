from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime

from execution.builder_auth import load_executor_config
from execution.order_policy import evaluate_order_policy
from execution.order_router import (
    OrderExecutionResult,
    execute_market_order,
    resolve_execution_mode,
)
from execution.polymarket_executor import fetch_market_snapshot, preview_market_order
from execution.runtime_guard import evaluate_runtime_guard
from execution.live_safety import evaluate_live_buy_safety
from execution.state_store import (
    has_signal,
    claim_signal,
    create_order_attempt,
    update_order_attempt,
    record_signal,
    get_open_position,
    upsert_buy_position,
    reduce_or_close_position,
    get_leader_registry,
    log_trade_event,
    list_open_positions,
)
from execution.trade_notifications import send_trade_notification
from risk.guards import build_runtime_risk_limits, evaluate_entry_risk
from risk.sizing import compute_signal_copy_amount


@dataclass
class LeaderSignal:
    signal_id: str
    leader_wallet: str
    token_id: str
    side: str
    leader_budget_usd: float
    leader_trade_size: float | None = None
    leader_trade_price: float | None = None
    leader_trade_notional_usd: float | None = None
    leader_portfolio_value_usd: float | None = None
    leader_token_position_size: float | None = None
    leader_token_position_value_usd: float | None = None
    leader_exit_fraction: float | None = None


def _parse_opened_at_to_minutes(opened_at: str | None) -> float | None:
    if not opened_at:
        return None
    try:
        dt = datetime.fromisoformat(opened_at.replace(" ", "T"))
        now = datetime.utcnow()
        return round((now - dt).total_seconds() / 60.0, 2)
    except Exception:
        return None


def _entry_price_drift_ok(
    *,
    leader_price: float | None,
    current_price: float | None,
    side: str,
    max_abs: float,
    max_rel: float,
) -> tuple[bool, str]:
    if leader_price is None or leader_price <= 0:
        return True, "leader trade price missing"
    if current_price is None or current_price <= 0:
        return False, "current price quote missing"

    abs_drift = abs(current_price - leader_price)
    rel_drift = abs_drift / leader_price
    side = side.upper()

    if side == "BUY" and current_price > leader_price:
        if abs_drift > max_abs:
            return False, f"buy price drift abs too high: {abs_drift:.4f} > {max_abs:.4f}"
        if rel_drift > max_rel:
            return False, f"buy price drift rel too high: {rel_drift:.4f} > {max_rel:.4f}"

    if side == "SELL" and current_price < leader_price:
        if abs_drift > max_abs:
            return False, f"sell price drift abs too high: {abs_drift:.4f} > {max_abs:.4f}"
        if rel_drift > max_rel:
            return False, f"sell price drift rel too high: {rel_drift:.4f} > {max_rel:.4f}"

    return True, "ok"


def _execute_with_recorded_attempt(
    *,
    config: dict,
    signal: LeaderSignal,
    amount_usd: float,
    reason: str,
) -> OrderExecutionResult:
    mode = resolve_execution_mode(config)
    attempt_id = create_order_attempt(
        signal_id=signal.signal_id,
        leader_wallet=signal.leader_wallet,
        token_id=signal.token_id,
        side=signal.side,
        amount_usd=amount_usd,
        mode=mode,
        status="RISK_APPROVED",
        reason=reason,
    )

    try:
        execution = execute_market_order(
            config=config,
            token_id=signal.token_id,
            amount_usd=amount_usd,
            side=signal.side,
            preview_fn=preview_market_order,
        )
    except Exception as e:
        execution = OrderExecutionResult(
            accepted=False,
            mode=mode,
            status="EXECUTION_ERROR",
            reason=str(e),
        )

    update_order_attempt(
        attempt_id=attempt_id,
        status=execution.status,
        reason=execution.reason,
        raw_response=execution.raw_response,
        order_id=execution.order_id,
        fill_amount_usd=execution.fill_amount_usd,
    )

    return execution


def _execution_failure_signal_status(execution: OrderExecutionResult) -> str:
    if execution.status == "LIVE_SUBMITTED_UNVERIFIED":
        return "EXECUTION_UNKNOWN"
    if execution.status in {"LIVE_SUBMIT_ERROR", "EXECUTION_ERROR"}:
        return "EXECUTION_ERROR"
    return "SKIPPED_EXECUTION"


def _entry_success_signal_status(execution: OrderExecutionResult) -> str:
    if execution.mode == "PAPER":
        return "PAPER_FILLED_ENTRY"
    if execution.mode == "LIVE":
        return "LIVE_FILLED_ENTRY"
    return "PREVIEW_READY_ENTRY"


def _exit_success_signal_status(execution: OrderExecutionResult, *, closed_fully: bool) -> str:
    if execution.mode == "PAPER":
        return "PAPER_FILLED_EXIT" if closed_fully else "PAPER_FILLED_PARTIAL_EXIT"
    if execution.mode == "LIVE":
        return "LIVE_FILLED_EXIT" if closed_fully else "LIVE_FILLED_PARTIAL_EXIT"
    return "PREVIEW_READY_EXIT" if closed_fully else "PREVIEW_READY_PARTIAL_EXIT"


def _execution_fill_price(execution: OrderExecutionResult, fallback_price: float | None) -> float | None:
    fill_price = execution.details.get("fill_price") if execution.details else None
    try:
        parsed = float(fill_price)
    except (TypeError, ValueError):
        parsed = 0.0
    return parsed if parsed > 0 else fallback_price


def _positive_float_or_none(value) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _safe_bool(value, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return bool(value)


def _open_wallet_exposure_usd(leader_wallet: str) -> float:
    total = 0.0
    for row in list_open_positions(limit=100000):
        if str(row.get("leader_wallet") or "") != leader_wallet:
            continue
        try:
            total += float(row.get("position_usd") or 0.0)
        except (TypeError, ValueError):
            continue
    return round(total, 8)


def _snapshot_min_order_size_usd(snapshot: dict, configured_min_order_usd: float) -> float:
    snapshot_min_order = _positive_float_or_none(snapshot.get("min_order_size"))
    if snapshot_min_order is None:
        return configured_min_order_usd
    price_quote = _positive_float_or_none(snapshot.get("price_quote"))
    if price_quote is None:
        return configured_min_order_usd
    return max(configured_min_order_usd, snapshot_min_order * price_quote)


def _sizing_max_per_trade_for_exit(config: dict, *, position_usd: float) -> float:
    risk = config.get("risk", {})
    explicit = _positive_float_or_none(risk.get("max_per_trade_usd"))
    if explicit is not None:
        return explicit

    fixed_capital_base = _positive_float_or_none(
        config.get("capital", {}).get("total_capital_usd")
    )
    pct = _positive_float_or_none(risk.get("max_per_trade_pct"))
    if pct is not None and fixed_capital_base is not None:
        return round(fixed_capital_base * pct, 8)

    return max(position_usd, float(risk.get("min_order_size_usd", 1.0)))


def process_signal(signal: LeaderSignal) -> dict:
    if has_signal(signal.signal_id):
        return {
            "signal_id": signal.signal_id,
            "status": "DUPLICATE",
            "reason": "signal already processed",
        }

    config = load_executor_config()
    runtime_guard = evaluate_runtime_guard(config=config)
    if not runtime_guard.allowed:
        if not claim_signal(
            signal_id=signal.signal_id,
            leader_wallet=signal.leader_wallet,
            token_id=signal.token_id,
            side=signal.side,
            leader_budget_usd=signal.leader_budget_usd,
        ):
            return {
                "signal_id": signal.signal_id,
                "status": "DUPLICATE",
                "reason": "signal already processed",
            }

        record_signal(
            signal_id=signal.signal_id,
            leader_wallet=signal.leader_wallet,
            token_id=signal.token_id,
            side=signal.side,
            leader_budget_usd=signal.leader_budget_usd,
            suggested_amount_usd=None,
            status="SKIPPED_RUNTIME",
            reason=runtime_guard.reason,
        )
        return {
            "signal": asdict(signal),
            "status": "SKIPPED_RUNTIME",
            "reason": runtime_guard.reason,
            "runtime_guard": asdict(runtime_guard),
        }

    risk = config.get("risk", {})
    filters = config.get("filters", {})
    sizing = config.get("sizing", {})
    exit_cfg = config.get("exit", {})
    freshness = config.get("signal_freshness", {})

    if signal.side.upper() == "BUY":
        live_safety = evaluate_live_buy_safety(config=config)
        if not live_safety.allowed:
            if not claim_signal(
                signal_id=signal.signal_id,
                leader_wallet=signal.leader_wallet,
                token_id=signal.token_id,
                side=signal.side,
                leader_budget_usd=signal.leader_budget_usd,
            ):
                return {
                    "signal_id": signal.signal_id,
                    "status": "DUPLICATE",
                    "reason": "signal already processed",
                }

            record_signal(
                signal_id=signal.signal_id,
                leader_wallet=signal.leader_wallet,
                token_id=signal.token_id,
                side=signal.side,
                leader_budget_usd=signal.leader_budget_usd,
                suggested_amount_usd=None,
                status="SKIPPED_LIVE_SAFETY",
                reason=live_safety.reason,
            )
            return {
                "signal": asdict(signal),
                "status": "SKIPPED_LIVE_SAFETY",
                "reason": live_safety.reason,
                "live_safety": asdict(live_safety),
            }

    registry = get_leader_registry(signal.leader_wallet)
    leader_user_name = registry["user_name"] if registry else None
    category = registry["category"] if registry else None
    leader_status = registry["leader_status"] if registry else None

    snapshot = fetch_market_snapshot(token_id=signal.token_id, side=signal.side)
    current_price = snapshot["price_quote"]

    if not claim_signal(
        signal_id=signal.signal_id,
        leader_wallet=signal.leader_wallet,
        token_id=signal.token_id,
        side=signal.side,
        leader_budget_usd=signal.leader_budget_usd,
    ):
        return {
            "signal_id": signal.signal_id,
            "status": "DUPLICATE",
            "reason": "signal already processed",
        }

    min_order_size_usd = float(risk.get("min_order_size_usd", 1.0))
    min_order_size_usd = _snapshot_min_order_size_usd(snapshot, min_order_size_usd)
    leader_trade_notional_copy_fraction = float(
        sizing.get("leader_trade_notional_copy_fraction", 0.20)
    )
    max_leader_trade_budget_fraction = _positive_float_or_none(
        sizing.get("max_leader_trade_budget_fraction")
    )
    allow_notional_fallback = _safe_bool(
        sizing.get("allow_notional_fallback"),
        False,
    )
    round_up_to_min_order = _safe_bool(
        sizing.get("round_up_to_min_order"),
        False,
    )
    allow_budget_fallback = _safe_bool(
        sizing.get("allow_budget_fallback"),
        False,
    )

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

        drift_ok = True
        drift_reason = "ok"
        if not bool(exit_cfg.get("ignore_exit_drift", True)):
            drift_ok, drift_reason = _entry_price_drift_ok(
                leader_price=signal.leader_trade_price,
                current_price=current_price,
                side=signal.side,
                max_abs=float(freshness.get("max_price_drift_abs", 0.01)),
                max_rel=float(freshness.get("max_price_drift_rel", 0.02)),
            )

        if not drift_ok:
            record_signal(
                signal_id=signal.signal_id,
                leader_wallet=signal.leader_wallet,
                token_id=signal.token_id,
                side=signal.side,
                leader_budget_usd=signal.leader_budget_usd,
                suggested_amount_usd=None,
                status="SKIPPED_DRIFT",
                reason=drift_reason,
            )
            return {
                "signal": asdict(signal),
                "market_snapshot": snapshot,
                "status": "SKIPPED_DRIFT",
                "reason": drift_reason,
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

        max_per_trade_usd = _sizing_max_per_trade_for_exit(
            config,
            position_usd=position_usd,
        )
        size_decision = compute_signal_copy_amount(
            leader_budget_usd=position_usd,
            remaining_leader_budget_usd=position_usd,
            leader_trade_notional_usd=signal.leader_trade_notional_usd,
            min_order_size_usd=min_order_size_usd,
            max_per_trade_usd=max_per_trade_usd,
            leader_trade_notional_copy_fraction=leader_trade_notional_copy_fraction,
            side=signal.side,
            leader_exit_fraction=signal.leader_exit_fraction,
            round_up_to_min_order=round_up_to_min_order,
            allow_notional_fallback=allow_notional_fallback,
            allow_budget_fallback=allow_budget_fallback,
        )
        if not size_decision.allowed:
            record_signal(
                signal_id=signal.signal_id,
                leader_wallet=signal.leader_wallet,
                token_id=signal.token_id,
                side=signal.side,
                leader_budget_usd=signal.leader_budget_usd,
                suggested_amount_usd=None,
                status="SKIPPED_SIZING",
                reason=size_decision.reason,
            )
            return {
                "signal": asdict(signal),
                "market_snapshot": snapshot,
                "status": "SKIPPED_SIZING",
                "reason": size_decision.reason,
                "sizing": asdict(size_decision),
            }

        suggested_sell_amount = size_decision.amount_usd
        sizing_source = size_decision.source
        sell_amount = min(position_usd, suggested_sell_amount)

        execution = _execute_with_recorded_attempt(
            config=config,
            signal=signal,
            amount_usd=round(sell_amount, 2),
            reason="exit policy approved",
        )

        if not execution.accepted:
            failure_status = _execution_failure_signal_status(execution)
            record_signal(
                signal_id=signal.signal_id,
                leader_wallet=signal.leader_wallet,
                token_id=signal.token_id,
                side=signal.side,
                leader_budget_usd=signal.leader_budget_usd,
                suggested_amount_usd=round(sell_amount, 2),
                status=failure_status,
                reason=execution.reason,
            )
            return {
                "signal": asdict(signal),
                "market_snapshot": snapshot,
                "status": failure_status,
                "reason": execution.reason,
                "suggested_amount_usd": round(sell_amount, 2),
                "execution": asdict(execution),
            }

        actual_execution_amount = execution.fill_amount_usd or sell_amount
        execution_price = _execution_fill_price(execution, current_price)

        reduced = reduce_or_close_position(
            leader_wallet=signal.leader_wallet,
            token_id=signal.token_id,
            signal_id=signal.signal_id,
            amount_usd=actual_execution_amount,
        )

        entry_avg_price = reduced["entry_avg_price"] if reduced else None
        position_before_usd = reduced["position_before_usd"] if reduced else position_usd
        position_after_usd = reduced["position_after_usd"] if reduced else 0.0
        actual_sell_amount = reduced["sell_amount_usd"] if reduced else sell_amount
        holding_minutes = _parse_opened_at_to_minutes(reduced["opened_at"] if reduced else None)
        closed_fully = bool(reduced["closed_fully"]) if reduced else True

        realized_pnl_usd = None
        realized_pnl_pct = None

        if entry_avg_price is not None and execution_price is not None:
            realized_pnl_pct = round((float(execution_price) - float(entry_avg_price)) / float(entry_avg_price), 6)
            realized_pnl_usd = round(actual_sell_amount * realized_pnl_pct, 4)

        notes = (
            f"{execution.mode.lower()} {'full' if closed_fully else 'partial'} exit generated "
            f"| sizing={sizing_source}"
        )

        log_trade_event(
            signal_id=signal.signal_id,
            leader_wallet=signal.leader_wallet,
            leader_user_name=leader_user_name,
            category=category,
            leader_status=leader_status,
            token_id=signal.token_id,
            side=signal.side,
            event_type="EXIT",
            amount_usd=round(actual_sell_amount, 2),
            price=execution_price,
            gross_value_usd=round(actual_sell_amount, 2),
            position_before_usd=position_before_usd,
            position_after_usd=position_after_usd,
            entry_avg_price=entry_avg_price,
            exit_price=execution_price,
            realized_pnl_usd=realized_pnl_usd,
            realized_pnl_pct=realized_pnl_pct,
            holding_minutes=holding_minutes,
            notes=notes,
        )

        status = _exit_success_signal_status(execution, closed_fully=closed_fully)
        send_trade_notification(
            config=config,
            mode=execution.mode,
            event_type="EXIT",
            leader_wallet=signal.leader_wallet,
            leader_user_name=leader_user_name,
            category=category,
            token_id=signal.token_id,
            amount_usd=round(actual_sell_amount, 2),
            price=execution_price,
            position_before_usd=position_before_usd,
            position_after_usd=position_after_usd,
            signal_id=signal.signal_id,
            realized_pnl_usd=realized_pnl_usd,
            realized_pnl_pct=realized_pnl_pct,
            holding_minutes=holding_minutes,
            closed_fully=closed_fully,
        )

        record_signal(
            signal_id=signal.signal_id,
            leader_wallet=signal.leader_wallet,
            token_id=signal.token_id,
            side=signal.side,
            leader_budget_usd=signal.leader_budget_usd,
            suggested_amount_usd=round(actual_sell_amount, 2),
            status=status,
            reason="ok",
        )

        return {
            "signal": asdict(signal),
            "market_snapshot": snapshot,
            "status": status,
            "reason": "ok",
            "suggested_amount_usd": round(actual_sell_amount, 2),
            "preview_order": execution.raw_response,
            "execution": asdict(execution),
            "realized_pnl_usd": realized_pnl_usd,
            "realized_pnl_pct": realized_pnl_pct,
            "holding_minutes": holding_minutes,
            "position_after_usd": position_after_usd,
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
        min_order_size_usd=min_order_size_usd,
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

    runtime_risk_limits = build_runtime_risk_limits(config)
    if runtime_risk_limits.capital_base_missing:
        reason = "risk percent limits require account collateral balance"
        if runtime_risk_limits.capital_base_error:
            reason = f"{reason}: {runtime_risk_limits.capital_base_error}"
        record_signal(
            signal_id=signal.signal_id,
            leader_wallet=signal.leader_wallet,
            token_id=signal.token_id,
            side=signal.side,
            leader_budget_usd=signal.leader_budget_usd,
            suggested_amount_usd=None,
            status="SKIPPED_RISK",
            reason=reason,
        )
        return {
            "signal": asdict(signal),
            "market_snapshot": snapshot,
            "status": "SKIPPED_RISK",
            "reason": reason,
            "risk": asdict(runtime_risk_limits),
        }

    wallet_exposure_usd = _open_wallet_exposure_usd(signal.leader_wallet)
    remaining_leader_budget_usd = max(signal.leader_budget_usd - wallet_exposure_usd, 0.0)

    size_decision = compute_signal_copy_amount(
        leader_budget_usd=signal.leader_budget_usd,
        remaining_leader_budget_usd=remaining_leader_budget_usd,
        leader_trade_notional_usd=signal.leader_trade_notional_usd,
        min_order_size_usd=min_order_size_usd,
        max_per_trade_usd=runtime_risk_limits.max_per_trade_usd,
        leader_trade_notional_copy_fraction=leader_trade_notional_copy_fraction,
        side=signal.side,
        leader_portfolio_value_usd=signal.leader_portfolio_value_usd,
        max_leader_trade_budget_fraction=max_leader_trade_budget_fraction,
        round_up_to_min_order=round_up_to_min_order,
        allow_notional_fallback=allow_notional_fallback,
        allow_budget_fallback=allow_budget_fallback,
    )

    if not size_decision.allowed:
        record_signal(
            signal_id=signal.signal_id,
            leader_wallet=signal.leader_wallet,
            token_id=signal.token_id,
            side=signal.side,
            leader_budget_usd=signal.leader_budget_usd,
            suggested_amount_usd=None,
            status="SKIPPED_SIZING",
            reason=size_decision.reason,
        )
        return {
            "signal": asdict(signal),
            "market_snapshot": snapshot,
            "status": "SKIPPED_SIZING",
            "reason": size_decision.reason,
            "sizing": asdict(size_decision),
        }

    amount_usd = size_decision.amount_usd
    sizing_source = size_decision.source

    drift_ok, drift_reason = _entry_price_drift_ok(
        leader_price=signal.leader_trade_price,
        current_price=current_price,
        side=signal.side,
        max_abs=float(freshness.get("max_price_drift_abs", 0.01)),
        max_rel=float(freshness.get("max_price_drift_rel", 0.02)),
    )

    if not drift_ok:
        record_signal(
            signal_id=signal.signal_id,
            leader_wallet=signal.leader_wallet,
            token_id=signal.token_id,
            side=signal.side,
            leader_budget_usd=signal.leader_budget_usd,
            suggested_amount_usd=amount_usd,
            status="SKIPPED_DRIFT",
            reason=drift_reason,
        )
        return {
            "signal": asdict(signal),
            "market_snapshot": snapshot,
            "status": "SKIPPED_DRIFT",
            "reason": drift_reason,
            "suggested_amount_usd": amount_usd,
        }

    risk_decision = evaluate_entry_risk(
        config=config,
        leader_wallet=signal.leader_wallet,
        token_id=signal.token_id,
        amount_usd=amount_usd,
        leader_budget_usd=signal.leader_budget_usd,
        category=category,
        limits=runtime_risk_limits,
    )

    if not risk_decision.allowed:
        record_signal(
            signal_id=signal.signal_id,
            leader_wallet=signal.leader_wallet,
            token_id=signal.token_id,
            side=signal.side,
            leader_budget_usd=signal.leader_budget_usd,
            suggested_amount_usd=amount_usd,
            status="SKIPPED_RISK",
            reason=risk_decision.reason,
        )
        return {
            "signal": asdict(signal),
            "market_snapshot": snapshot,
            "status": "SKIPPED_RISK",
            "reason": risk_decision.reason,
            "suggested_amount_usd": amount_usd,
            "risk": asdict(risk_decision),
        }

    execution = _execute_with_recorded_attempt(
        config=config,
        signal=signal,
        amount_usd=amount_usd,
        reason="entry policy and risk approved",
    )

    if not execution.accepted:
        failure_status = _execution_failure_signal_status(execution)
        record_signal(
            signal_id=signal.signal_id,
            leader_wallet=signal.leader_wallet,
            token_id=signal.token_id,
            side=signal.side,
            leader_budget_usd=signal.leader_budget_usd,
            suggested_amount_usd=amount_usd,
            status=failure_status,
            reason=execution.reason,
        )
        return {
            "signal": asdict(signal),
            "market_snapshot": snapshot,
            "status": failure_status,
            "reason": execution.reason,
            "suggested_amount_usd": amount_usd,
            "execution": asdict(execution),
        }

    executed_amount_usd = execution.fill_amount_usd or amount_usd
    execution_price = _execution_fill_price(execution, current_price)

    pos_update = upsert_buy_position(
        leader_wallet=signal.leader_wallet,
        token_id=signal.token_id,
        amount_usd=executed_amount_usd,
        entry_price=execution_price,
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
        amount_usd=executed_amount_usd,
        price=execution_price,
        gross_value_usd=executed_amount_usd,
        position_before_usd=pos_update["position_before_usd"],
        position_after_usd=pos_update["position_after_usd"],
        entry_avg_price=pos_update["entry_avg_price_after"],
        exit_price=None,
        realized_pnl_usd=None,
        realized_pnl_pct=None,
        holding_minutes=None,
        notes=f"{execution.mode.lower()} entry generated | sizing={sizing_source}",
    )

    entry_status = _entry_success_signal_status(execution)
    send_trade_notification(
        config=config,
        mode=execution.mode,
        event_type="ENTRY",
        leader_wallet=signal.leader_wallet,
        leader_user_name=leader_user_name,
        category=category,
        token_id=signal.token_id,
        amount_usd=executed_amount_usd,
        price=execution_price,
        position_before_usd=pos_update["position_before_usd"],
        position_after_usd=pos_update["position_after_usd"],
        signal_id=signal.signal_id,
    )

    record_signal(
        signal_id=signal.signal_id,
        leader_wallet=signal.leader_wallet,
        token_id=signal.token_id,
        side=signal.side,
        leader_budget_usd=signal.leader_budget_usd,
        suggested_amount_usd=executed_amount_usd,
        status=entry_status,
        reason="ok",
    )

    return {
        "signal": asdict(signal),
        "market_snapshot": snapshot,
        "status": entry_status,
        "reason": "ok",
        "suggested_amount_usd": executed_amount_usd,
        "preview_order": execution.raw_response,
        "execution": asdict(execution),
    }
