from __future__ import annotations

import json
from typing import Any, Callable

from execution.order_router import parse_live_order_response
from execution.polymarket_executor import build_authenticated_client
from execution.state_store import (
    get_leader_registry,
    get_open_position,
    log_trade_event,
    record_signal,
    reduce_or_close_position,
    update_order_attempt,
    upsert_buy_position,
)


UNVERIFIED_LIVE_STATUSES = {"LIVE_SUBMITTED_UNVERIFIED"}
RECOVERY_APPLY_ACK = "APPLY_VERIFIED_LIVE_RECOVERY"


def _safe_float(value: Any) -> float:
    try:
        return float(value) if value is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _extract_order_id_from_raw(raw_response_json: str | None) -> str | None:
    if not raw_response_json:
        return None

    try:
        payload = json.loads(raw_response_json)
    except json.JSONDecodeError:
        return None

    def find(payload):
        if isinstance(payload, dict):
            for key in ("orderID", "orderId", "order_id", "id"):
                value = payload.get(key)
                if value not in (None, ""):
                    return str(value)
            for value in payload.values():
                found = find(value)
                if found:
                    return found
        elif isinstance(payload, list):
            for item in payload:
                found = find(item)
                if found:
                    return found
        return None

    return find(payload)


def fetch_order_status(order_id: str) -> dict[str, Any]:
    client = build_authenticated_client()
    raw = client.get_order(order_id)
    if not isinstance(raw, dict):
        return {"raw_response": raw}
    return raw


def build_unverified_order_recovery_report(
    *,
    order_attempt_rows: list[dict[str, Any]],
    order_status_fetcher: Callable[[str], dict[str, Any]] = fetch_order_status,
) -> list[dict[str, Any]]:
    report_rows: list[dict[str, Any]] = []

    for attempt in order_attempt_rows:
        if attempt.get("status") not in UNVERIFIED_LIVE_STATUSES:
            continue

        order_id = attempt.get("order_id") or _extract_order_id_from_raw(
            attempt.get("raw_response_json")
        )
        base = {
            "attempt_id": attempt.get("attempt_id"),
            "signal_id": attempt.get("signal_id"),
            "leader_wallet": attempt.get("leader_wallet"),
            "token_id": attempt.get("token_id"),
            "side": attempt.get("side"),
            "amount_usd": _safe_float(attempt.get("amount_usd")),
            "order_id": order_id or "",
        }

        if not order_id:
            report_rows.append(
                {
                    **base,
                    "recovery_status": "ORDER_ID_MISSING",
                    "recovery_reason": "cannot fetch order status without order_id",
                    "verified_fill_amount_usd": 0.0,
                }
            )
            continue

        try:
            raw_status = order_status_fetcher(str(order_id))
        except Exception as e:
            report_rows.append(
                {
                    **base,
                    "recovery_status": "FETCH_FAILED",
                    "recovery_reason": str(e),
                    "verified_fill_amount_usd": 0.0,
                }
            )
            continue

        parsed = parse_live_order_response(
            raw_response=raw_status,
            requested_amount_usd=_safe_float(attempt.get("amount_usd")),
            require_verified_fill=True,
        )
        recovery_status = "FILL_VERIFIED" if parsed.accepted else parsed.status
        fill_price = parsed.details.get("fill_price")

        report_rows.append(
            {
                **base,
                "recovery_status": recovery_status,
                "recovery_reason": parsed.reason,
                "verified_fill_amount_usd": parsed.fill_amount_usd,
                "verified_fill_price": fill_price if fill_price is not None else "",
            }
        )

    return report_rows


def _success_signal_status(side: str, *, closed_fully: bool | None = None) -> str:
    if side.upper() == "BUY":
        return "LIVE_FILLED_ENTRY"
    if closed_fully:
        return "LIVE_FILLED_EXIT"
    return "LIVE_FILLED_PARTIAL_EXIT"


def apply_unverified_order_recovery(
    *,
    order_attempt_rows: list[dict[str, Any]],
    apply: bool = False,
    ack: str = "",
    order_status_fetcher: Callable[[str], dict[str, Any]] = fetch_order_status,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    if apply and ack != RECOVERY_APPLY_ACK:
        raise ValueError("recovery apply ack is missing or invalid")

    for attempt in order_attempt_rows:
        if attempt.get("status") not in UNVERIFIED_LIVE_STATUSES:
            continue

        order_id = attempt.get("order_id") or _extract_order_id_from_raw(
            attempt.get("raw_response_json")
        )
        base = {
            "attempt_id": attempt.get("attempt_id"),
            "signal_id": attempt.get("signal_id"),
            "leader_wallet": attempt.get("leader_wallet"),
            "token_id": attempt.get("token_id"),
            "side": str(attempt.get("side") or "").upper(),
            "amount_usd": _safe_float(attempt.get("amount_usd")),
            "order_id": order_id or "",
            "applied": False,
        }

        if not order_id:
            rows.append(
                {
                    **base,
                    "recovery_status": "ORDER_ID_MISSING",
                    "recovery_reason": "cannot fetch order status without order_id",
                }
            )
            continue

        try:
            raw_status = order_status_fetcher(str(order_id))
        except Exception as e:
            rows.append(
                {
                    **base,
                    "recovery_status": "FETCH_FAILED",
                    "recovery_reason": str(e),
                }
            )
            continue

        parsed = parse_live_order_response(
            raw_response=raw_status,
            requested_amount_usd=_safe_float(attempt.get("amount_usd")),
            require_verified_fill=True,
        )

        if not parsed.accepted:
            rows.append(
                {
                    **base,
                    "recovery_status": parsed.status,
                    "recovery_reason": parsed.reason,
                    "verified_fill_amount_usd": parsed.fill_amount_usd,
                    "verified_fill_price": parsed.details.get("fill_price") or "",
                }
            )
            continue

        fill_price = parsed.details.get("fill_price")
        if fill_price is None or _safe_float(fill_price) <= 0:
            rows.append(
                {
                    **base,
                    "recovery_status": "FILL_VERIFIED_PRICE_MISSING",
                    "recovery_reason": "verified fill amount found, but fill price is required to update local state",
                    "verified_fill_amount_usd": parsed.fill_amount_usd,
                    "verified_fill_price": "",
                }
            )
            continue

        side = base["side"]
        if side not in {"BUY", "SELL"}:
            rows.append(
                {
                    **base,
                    "recovery_status": "UNSUPPORTED_SIDE",
                    "recovery_reason": f"unsupported side: {side}",
                    "verified_fill_amount_usd": parsed.fill_amount_usd,
                    "verified_fill_price": fill_price,
                }
            )
            continue

        result = {
            **base,
            "recovery_status": "FILL_VERIFIED",
            "recovery_reason": parsed.reason,
            "verified_fill_amount_usd": parsed.fill_amount_usd,
            "verified_fill_price": fill_price,
        }

        if not apply:
            rows.append(result)
            continue

        leader_wallet = str(attempt["leader_wallet"])
        token_id = str(attempt["token_id"])
        signal_id = str(attempt["signal_id"])
        registry = get_leader_registry(leader_wallet)
        leader_user_name = registry.get("user_name") if registry else None
        category = registry.get("category") if registry else None
        leader_status = registry.get("leader_status") if registry else None

        if side == "BUY":
            pos_update = upsert_buy_position(
                leader_wallet=leader_wallet,
                token_id=token_id,
                amount_usd=parsed.fill_amount_usd,
                entry_price=float(fill_price),
                signal_id=signal_id,
            )
            closed_fully = None
            position_before = pos_update["position_before_usd"]
            position_after = pos_update["position_after_usd"]
            entry_avg_price = pos_update["entry_avg_price_after"]
            realized_pnl_usd = None
            realized_pnl_pct = None
            event_type = "ENTRY"
        else:
            open_position = get_open_position(leader_wallet, token_id)
            if open_position is None:
                rows.append(
                    {
                        **result,
                        "recovery_status": "SELL_WITHOUT_OPEN_POSITION",
                        "recovery_reason": "cannot apply recovered SELL without local open position",
                    }
                )
                continue

            reduced = reduce_or_close_position(
                leader_wallet=leader_wallet,
                token_id=token_id,
                signal_id=signal_id,
                amount_usd=parsed.fill_amount_usd,
            )
            if reduced is None:
                rows.append(
                    {
                        **result,
                        "recovery_status": "SELL_REDUCE_FAILED",
                        "recovery_reason": "failed to reduce local open position",
                    }
                )
                continue

            closed_fully = bool(reduced["closed_fully"])
            position_before = reduced["position_before_usd"]
            position_after = reduced["position_after_usd"]
            entry_avg_price = reduced["entry_avg_price"]
            realized_pnl_pct = None
            realized_pnl_usd = None
            if entry_avg_price and entry_avg_price > 0:
                realized_pnl_pct = round((float(fill_price) - float(entry_avg_price)) / float(entry_avg_price), 6)
                realized_pnl_usd = round(float(reduced["sell_amount_usd"]) * realized_pnl_pct, 4)
            event_type = "EXIT"

        update_order_attempt(
            attempt_id=int(attempt["attempt_id"]),
            status="LIVE_FILLED_RECOVERED",
            reason="verified live fill recovered from exchange order status",
            raw_response=raw_status,
            order_id=parsed.order_id or str(order_id),
            fill_amount_usd=parsed.fill_amount_usd,
        )

        signal_status = _success_signal_status(side, closed_fully=closed_fully)
        record_signal(
            signal_id=signal_id,
            leader_wallet=leader_wallet,
            token_id=token_id,
            side=side,
            leader_budget_usd=_safe_float(attempt.get("leader_budget_usd")),
            suggested_amount_usd=parsed.fill_amount_usd,
            status=signal_status,
            reason="ok recovered live fill",
        )

        log_trade_event(
            signal_id=signal_id,
            leader_wallet=leader_wallet,
            leader_user_name=leader_user_name,
            category=category,
            leader_status=leader_status,
            token_id=token_id,
            side=side,
            event_type=event_type,
            amount_usd=parsed.fill_amount_usd,
            price=float(fill_price),
            gross_value_usd=parsed.fill_amount_usd,
            position_before_usd=position_before,
            position_after_usd=position_after,
            entry_avg_price=entry_avg_price,
            exit_price=float(fill_price) if side == "SELL" else None,
            realized_pnl_usd=realized_pnl_usd,
            realized_pnl_pct=realized_pnl_pct,
            holding_minutes=None,
            notes="live fill recovered from exchange order status",
        )

        rows.append({**result, "applied": True, "signal_status": signal_status})

    return rows
