from __future__ import annotations

import hashlib
from dataclasses import asdict
from datetime import datetime
from typing import Any, Callable

from execution.builder_auth import load_executor_config
from execution.order_router import OrderExecutionResult, execute_market_order, resolve_execution_mode
from execution.polymarket_executor import (
    fetch_market_snapshot,
    preview_market_order_shares,
    submit_live_market_order_shares,
)
from execution.position_marking import mark_position
from execution.runtime_guard import evaluate_runtime_guard
from execution.state_backup import backup_state_db
from execution.state_store import (
    claim_signal,
    create_order_attempt,
    init_db,
    list_leader_registry,
    list_open_positions,
    log_trade_event,
    record_signal,
    reduce_or_close_position,
    update_order_attempt,
)
from execution.trade_notifications import send_trade_notification


SnapshotLoader = Callable[[str, str], dict[str, Any]]
ShareOrderFn = Callable[..., dict[str, Any]]
NotificationFn = Callable[..., Any]


def _safe_float(value: Any) -> float:
    try:
        return float(value) if value is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _short(value: Any, left: int = 8, right: int = 6) -> str:
    text = str(value or "")
    if len(text) <= left + right + 3:
        return text
    return f"{text[:left]}...{text[-right:]}"


def _registry_by_wallet() -> dict[str, dict[str, Any]]:
    return {str(row.get("wallet") or ""): row for row in list_leader_registry(limit=100000)}


def _leader_label(wallet: str, registry: dict[str, Any] | None) -> str:
    if registry and registry.get("user_name"):
        return str(registry["user_name"])
    return _short(wallet)


def _holding_minutes(opened_at: Any) -> float | None:
    if not opened_at:
        return None
    try:
        dt = datetime.fromisoformat(str(opened_at).replace(" ", "T"))
        return round((datetime.utcnow() - dt).total_seconds() / 60.0, 2)
    except Exception:
        return None


def _manual_signal_id(position: dict[str, Any]) -> str:
    raw = "|".join(
        [
            "manual_unwind",
            str(position.get("leader_wallet") or ""),
            str(position.get("token_id") or ""),
            str(position.get("opened_at") or ""),
            str(position.get("updated_at") or ""),
        ]
    )
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
    return f"manual_unwind:{digest}"


def _success_status(execution: OrderExecutionResult) -> str:
    if execution.mode == "PAPER":
        return "PAPER_FILLED_MANUAL_EXIT"
    if execution.mode == "LIVE":
        return "LIVE_FILLED_MANUAL_EXIT"
    return "PREVIEW_READY_MANUAL_EXIT"


def _failure_status(execution: OrderExecutionResult) -> str:
    if execution.status == "LIVE_SUBMITTED_UNVERIFIED":
        return "EXECUTION_UNKNOWN"
    if execution.status in {"LIVE_SUBMIT_ERROR", "EXECUTION_ERROR"}:
        return "EXECUTION_ERROR"
    return "SKIPPED_EXECUTION"


def list_unwind_targets() -> list[dict[str, Any]]:
    init_db()
    registry = _registry_by_wallet()
    grouped: dict[str, dict[str, Any]] = {}

    for position in list_open_positions(limit=100000):
        wallet = str(position.get("leader_wallet") or "")
        if not wallet:
            continue
        target = grouped.setdefault(
            wallet,
            {
                "wallet": wallet,
                "user_name": _leader_label(wallet, registry.get(wallet)),
                "category": (registry.get(wallet) or {}).get("category"),
                "positions": 0,
                "position_usd": 0.0,
            },
        )
        target["positions"] += 1
        target["position_usd"] += _safe_float(position.get("position_usd"))

    rows = list(grouped.values())
    for row in rows:
        row["position_usd"] = round(float(row["position_usd"]), 2)
    rows.sort(key=lambda row: float(row["position_usd"]), reverse=True)
    return rows


def select_unwind_positions(target_wallet: str | None = None) -> list[dict[str, Any]]:
    positions = list_open_positions(limit=100000)
    if target_wallet:
        target = str(target_wallet).lower()
        positions = [
            row
            for row in positions
            if str(row.get("leader_wallet") or "").lower() == target
        ]
    positions.sort(
        key=lambda row: (
            str(row.get("leader_wallet") or ""),
            -_safe_float(row.get("position_usd")),
        )
    )
    return positions


def build_unwind_preview(target_wallet: str | None = None) -> dict[str, Any]:
    init_db()
    registry = _registry_by_wallet()
    positions = select_unwind_positions(target_wallet)
    total_position = sum(_safe_float(row.get("position_usd")) for row in positions)
    wallets = sorted({str(row.get("leader_wallet") or "") for row in positions})
    leader_names = [
        _leader_label(wallet, registry.get(wallet))
        for wallet in wallets[:8]
    ]
    return {
        "scope": "ALL" if target_wallet is None else str(target_wallet),
        "positions": len(positions),
        "leaders": len(wallets),
        "leader_names": leader_names,
        "position_usd": round(total_position, 2),
    }


def _execute_share_sell(
    *,
    config: dict[str, Any],
    token_id: str,
    market_value_usd: float,
    share_amount: float,
    preview_share_fn: ShareOrderFn,
    live_share_fn: ShareOrderFn,
) -> OrderExecutionResult:
    mode = resolve_execution_mode(config)

    def preview_fn(*, token_id: str, amount_usd: float, side: str) -> dict[str, Any]:
        return preview_share_fn(token_id=token_id, share_amount=share_amount, side=side)

    def live_fn(*, token_id: str, amount_usd: float, side: str) -> dict[str, Any]:
        return live_share_fn(token_id=token_id, share_amount=share_amount, side=side)

    return execute_market_order(
        config=config,
        token_id=token_id,
        amount_usd=round(market_value_usd, 8),
        side="SELL",
        preview_fn=preview_fn,
        live_order_fn=live_fn,
    )


def _execute_position_unwind(
    *,
    position: dict[str, Any],
    config: dict[str, Any],
    snapshot_loader: SnapshotLoader,
    preview_share_fn: ShareOrderFn,
    live_share_fn: ShareOrderFn,
    notification_fn: NotificationFn,
) -> dict[str, Any]:
    wallet = str(position.get("leader_wallet") or "")
    token_id = str(position.get("token_id") or "")
    position_usd = _safe_float(position.get("position_usd"))
    avg_entry_price = _safe_float(position.get("avg_entry_price"))
    signal_id = _manual_signal_id(position)

    base = {
        "signal_id": signal_id,
        "leader_wallet": wallet,
        "token_id": token_id,
        "position_usd": round(position_usd, 8),
        "avg_entry_price": avg_entry_price,
    }

    if position_usd <= 0 or avg_entry_price <= 0:
        return {**base, "status": "SKIPPED_INVALID_POSITION", "reason": "position_usd or avg_entry_price is not positive"}

    marked = mark_position(position, snapshot_loader=snapshot_loader, snapshot_side="SELL")
    if str(marked.get("snapshot_status") or "") == "SETTLED":
        return {**base, "status": "SKIPPED_SETTLEMENT_REQUIRED", "reason": str(marked.get("snapshot_reason") or "resolved market requires settlement")}
    if str(marked.get("snapshot_status") or "") != "OK":
        return {**base, "status": "SKIPPED_SNAPSHOT_ERROR", "reason": str(marked.get("snapshot_reason") or "snapshot failed")}

    sell_price = _safe_float(marked.get("best_bid"))
    if sell_price <= 0:
        return {**base, "status": "SKIPPED_NO_BID", "reason": "no positive best bid for market sell"}

    share_amount = position_usd / avg_entry_price
    market_value_usd = share_amount * sell_price

    if not claim_signal(
        signal_id=signal_id,
        leader_wallet=wallet,
        token_id=token_id,
        side="SELL",
        leader_budget_usd=position_usd,
    ):
        return {**base, "status": "DUPLICATE", "reason": "manual unwind already processed for this position version"}

    mode = resolve_execution_mode(config)
    attempt_id = create_order_attempt(
        signal_id=signal_id,
        leader_wallet=wallet,
        token_id=token_id,
        side="SELL",
        amount_usd=round(market_value_usd, 8),
        mode=mode,
        status="RISK_APPROVED",
        reason="manual telegram market unwind",
    )

    try:
        execution = _execute_share_sell(
            config=config,
            token_id=token_id,
            market_value_usd=market_value_usd,
            share_amount=share_amount,
            preview_share_fn=preview_share_fn,
            live_share_fn=live_share_fn,
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

    if not execution.accepted:
        status = _failure_status(execution)
        record_signal(
            signal_id=signal_id,
            leader_wallet=wallet,
            token_id=token_id,
            side="SELL",
            leader_budget_usd=position_usd,
            suggested_amount_usd=round(position_usd, 2),
            status=status,
            reason=execution.reason,
        )
        return {
            **base,
            "status": status,
            "reason": execution.reason,
            "market_value_usd": round(market_value_usd, 8),
            "share_amount": round(share_amount, 8),
            "execution": asdict(execution),
        }

    proceeds_usd = execution.fill_amount_usd or market_value_usd
    fill_price = _safe_float(execution.details.get("fill_price")) if execution.details else 0.0
    execution_price = fill_price if fill_price > 0 else proceeds_usd / share_amount

    reduced = reduce_or_close_position(
        leader_wallet=wallet,
        token_id=token_id,
        signal_id=signal_id,
        amount_usd=position_usd,
    )
    sold_cost_basis = _safe_float((reduced or {}).get("sell_amount_usd")) or position_usd
    position_before_usd = _safe_float((reduced or {}).get("position_before_usd")) or position_usd
    position_after_usd = _safe_float((reduced or {}).get("position_after_usd"))
    closed_fully = bool((reduced or {}).get("closed_fully", True))
    realized_pnl_usd = round(proceeds_usd - sold_cost_basis, 4)
    realized_pnl_pct = round(realized_pnl_usd / sold_cost_basis, 6) if sold_cost_basis > 0 else None

    registry = _registry_by_wallet().get(wallet)
    leader_user_name = _leader_label(wallet, registry)
    category = (registry or {}).get("category")
    leader_status = (registry or {}).get("leader_status")
    holding_minutes = _holding_minutes((reduced or {}).get("opened_at") or position.get("opened_at"))

    log_trade_event(
        signal_id=signal_id,
        leader_wallet=wallet,
        leader_user_name=leader_user_name,
        category=category,
        leader_status=leader_status,
        token_id=token_id,
        side="SELL",
        event_type="EXIT",
        amount_usd=round(sold_cost_basis, 2),
        price=execution_price,
        gross_value_usd=round(proceeds_usd, 2),
        position_before_usd=position_before_usd,
        position_after_usd=position_after_usd,
        entry_avg_price=avg_entry_price,
        exit_price=execution_price,
        realized_pnl_usd=realized_pnl_usd,
        realized_pnl_pct=realized_pnl_pct,
        holding_minutes=holding_minutes,
        notes=(
            "manual telegram market unwind "
            f"| shares={share_amount:.8f} | proceeds_usd={proceeds_usd:.8f}"
        ),
    )

    notification_fn(
        config=config,
        mode=execution.mode,
        event_type="EXIT",
        leader_wallet=wallet,
        leader_user_name=leader_user_name,
        category=category,
        token_id=token_id,
        amount_usd=round(sold_cost_basis, 2),
        price=execution_price,
        position_before_usd=position_before_usd,
        position_after_usd=position_after_usd,
        signal_id=signal_id,
        realized_pnl_usd=realized_pnl_usd,
        realized_pnl_pct=realized_pnl_pct,
        holding_minutes=holding_minutes,
        closed_fully=closed_fully,
    )

    status = _success_status(execution)
    record_signal(
        signal_id=signal_id,
        leader_wallet=wallet,
        token_id=token_id,
        side="SELL",
        leader_budget_usd=position_usd,
        suggested_amount_usd=round(sold_cost_basis, 2),
        status=status,
        reason="manual telegram market unwind",
    )

    return {
        **base,
        "status": status,
        "reason": "ok",
        "mode": execution.mode,
        "leader_user_name": leader_user_name,
        "category": category,
        "market_value_usd": round(market_value_usd, 8),
        "proceeds_usd": round(proceeds_usd, 8),
        "share_amount": round(share_amount, 8),
        "execution_price": execution_price,
        "realized_pnl_usd": realized_pnl_usd,
        "realized_pnl_pct": realized_pnl_pct,
        "position_after_usd": position_after_usd,
        "closed_fully": closed_fully,
        "execution": asdict(execution),
    }


def execute_manual_unwind(
    *,
    target_wallet: str | None = None,
    config: dict[str, Any] | None = None,
    snapshot_loader: SnapshotLoader = fetch_market_snapshot,
    preview_share_fn: ShareOrderFn = preview_market_order_shares,
    live_share_fn: ShareOrderFn = submit_live_market_order_shares,
    notification_fn: NotificationFn = send_trade_notification,
) -> dict[str, Any]:
    init_db()
    config = config or load_executor_config()
    guard = evaluate_runtime_guard(config=config)
    if not guard.allowed:
        return {
            "status": "BLOCKED_RUNTIME",
            "reason": guard.reason,
            "target_wallet": target_wallet,
            "positions": 0,
            "results": [],
            "runtime_guard": asdict(guard),
        }

    backup = backup_state_db(config=config, label="manual_unwind_before")
    positions = select_unwind_positions(target_wallet)
    results = [
        _execute_position_unwind(
            position=position,
            config=config,
            snapshot_loader=snapshot_loader,
            preview_share_fn=preview_share_fn,
            live_share_fn=live_share_fn,
            notification_fn=notification_fn,
        )
        for position in positions
    ]
    success = sum(1 for row in results if "MANUAL_EXIT" in str(row.get("status") or ""))
    failed = sum(1 for row in results if row.get("status") not in {"DUPLICATE"} and "MANUAL_EXIT" not in str(row.get("status") or ""))
    realized = sum(_safe_float(row.get("realized_pnl_usd")) for row in results)
    proceeds = sum(_safe_float(row.get("proceeds_usd")) for row in results)

    return {
        "status": "OK" if failed == 0 else "PARTIAL",
        "reason": "ok" if failed == 0 else "some positions were not unwound",
        "target_wallet": target_wallet,
        "positions": len(positions),
        "success": success,
        "failed": failed,
        "realized_pnl_usd": round(realized, 4),
        "proceeds_usd": round(proceeds, 4),
        "backup": asdict(backup),
        "results": results,
    }


def format_unwind_result(summary: dict[str, Any]) -> str:
    lines = [
        "Ручной выход по рынку",
        f"status: {summary.get('status')} | positions: {summary.get('positions')} | filled: {summary.get('success')} | skipped/failed: {summary.get('failed')}",
        f"proceeds: ${_safe_float(summary.get('proceeds_usd')):.2f} | realized: ${_safe_float(summary.get('realized_pnl_usd')):.2f}",
    ]
    backup = summary.get("backup") or {}
    if backup.get("created"):
        lines.append(f"backup: {backup.get('backup_path')}")
    if summary.get("reason") and summary.get("reason") != "ok":
        lines.append(f"reason: {summary.get('reason')}")

    rows = list(summary.get("results") or [])
    if rows:
        lines.append("")
        lines.append("Позиции:")
    for idx, row in enumerate(rows[:12], start=1):
        pnl = _safe_float(row.get("realized_pnl_usd"))
        pnl_sign = "+" if pnl > 0 else ""
        leader = row.get("leader_user_name") or _short(row.get("leader_wallet"))
        lines.append(
            f"{idx}. {leader} | {row.get('status')} | "
            f"cost ${_safe_float(row.get('position_usd')):.2f} | "
            f"proceeds ${_safe_float(row.get('proceeds_usd')):.2f} | "
            f"PnL {pnl_sign}${pnl:.2f} | token {_short(row.get('token_id'))}"
        )
        if row.get("reason") and row.get("reason") != "ok":
            lines.append(f"   {str(row.get('reason'))[:120]}")
    if len(rows) > 12:
        lines.append(f"... еще {len(rows) - 12} позиций")
    return "\n".join(lines)
