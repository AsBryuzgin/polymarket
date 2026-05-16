from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable

from execution.allowance import PreflightDecision, evaluate_live_funding_preflight
from execution.polymarket_executor import fetch_live_order_status, submit_live_market_order


LIVE_TRADING_ACK = "I_UNDERSTAND_THIS_PLACES_REAL_ORDERS"


@dataclass(frozen=True)
class OrderExecutionResult:
    accepted: bool
    mode: str
    status: str
    reason: str
    raw_response: dict[str, Any] | None = None
    fill_amount_usd: float = 0.0
    order_id: str | None = None
    details: dict[str, Any] = field(default_factory=dict)


def _bool_or_default(value: Any, default: bool) -> bool:
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


def resolve_execution_mode(config: dict[str, Any]) -> str:
    global_cfg = config.get("global", {})
    requested = str(global_cfg.get("execution_mode", "")).strip().upper()
    simulation = _bool_or_default(global_cfg.get("simulation"), True)
    preview_mode = _bool_or_default(global_cfg.get("preview_mode"), True)

    if preview_mode:
        return "PREVIEW"
    if simulation:
        return "PAPER"
    if requested == "LIVE":
        return "LIVE"
    if requested == "PAPER":
        return "PAPER"
    return "PREVIEW"


def _live_enabled(config: dict[str, Any]) -> tuple[bool, str]:
    global_cfg = config.get("global", {})
    enabled = _bool_or_default(global_cfg.get("live_trading_enabled"), False)
    ack = str(global_cfg.get("live_trading_ack", ""))

    if not enabled:
        return False, "live trading disabled by config"
    if ack != LIVE_TRADING_ACK:
        return False, "live trading ack is missing or invalid"
    return True, "ok"


def _find_first_value(payload: Any, keys: set[str]) -> Any:
    if isinstance(payload, dict):
        for key, value in payload.items():
            if str(key) in keys and value not in (None, ""):
                return value
        for value in payload.values():
            found = _find_first_value(value, keys)
            if found not in (None, ""):
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _find_first_value(item, keys)
            if found not in (None, ""):
                return found
    return None


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_verified_fill(raw_response: dict[str, Any]) -> tuple[float | None, float | None]:
    fill_amount = _safe_float(
        _find_first_value(
            raw_response,
            {
                "filled_amount_usd",
                "filledAmountUsd",
                "fill_amount_usd",
                "fillAmountUsd",
                "matched_amount_usd",
                "matchedAmountUsd",
                "amount_filled_usd",
                "amountFilledUsd",
                "filled_notional_usd",
                "filledNotionalUsd",
            },
        )
    )

    fill_price = _safe_float(
        _find_first_value(
            raw_response,
            {
                "avg_fill_price",
                "avgFillPrice",
                "fill_price",
                "fillPrice",
                "matched_price",
                "matchedPrice",
                "price",
            },
        )
    )

    if fill_amount is not None and fill_amount > 0:
        return fill_amount, fill_price

    size_matched = _safe_float(
        _find_first_value(
            raw_response,
            {
                "size_matched",
                "sizeMatched",
                "matched_size",
                "matchedSize",
                "filled_size",
                "filledSize",
            },
        )
    )

    if size_matched is not None and size_matched > 0 and fill_price is not None and fill_price > 0:
        return size_matched * fill_price, fill_price

    return None, fill_price


def parse_live_order_response(
    *,
    raw_response: dict[str, Any],
    requested_amount_usd: float,
    require_verified_fill: bool,
) -> OrderExecutionResult:
    order_id = _find_first_value(
        raw_response,
        {"orderID", "orderId", "order_id", "id"},
    )
    status_raw = _find_first_value(
        raw_response,
        {"status", "state", "orderStatus"},
    )
    status = str(status_raw or "").upper()
    success_raw = _find_first_value(raw_response, {"success", "ok", "accepted"})
    success = _bool_or_default(success_raw, False) if success_raw is not None else None

    fill_amount, fill_price = _extract_verified_fill(raw_response)

    details = {
        "requested_amount_usd": round(requested_amount_usd, 8),
        "raw_status": status_raw,
        "success": success,
        "fill_price": round(fill_price, 8) if fill_price is not None else None,
    }

    if status in {"REJECTED", "FAILED", "CANCELLED", "CANCELED", "EXPIRED"} or success is False:
        return OrderExecutionResult(
            accepted=False,
            mode="LIVE",
            status="LIVE_REJECTED",
            reason=f"live order rejected or not accepted: status={status_raw}",
            raw_response=raw_response,
            order_id=str(order_id) if order_id is not None else None,
            details=details,
        )

    if fill_amount is not None and fill_amount > 0:
        return OrderExecutionResult(
            accepted=True,
            mode="LIVE",
            status="LIVE_FILLED",
            reason="live order fill verified",
            raw_response=raw_response,
            fill_amount_usd=round(fill_amount, 8),
            order_id=str(order_id) if order_id is not None else None,
            details=details,
        )

    if not require_verified_fill and (success is True or status in {"FILLED", "MATCHED"}):
        return OrderExecutionResult(
            accepted=True,
            mode="LIVE",
            status="LIVE_FILLED_UNVERIFIED_AMOUNT",
            reason="live order appears filled; using requested amount because verified fill amount is unavailable",
            raw_response=raw_response,
            fill_amount_usd=round(requested_amount_usd, 8),
            order_id=str(order_id) if order_id is not None else None,
            details=details,
        )

    return OrderExecutionResult(
        accepted=False,
        mode="LIVE",
        status="LIVE_SUBMITTED_UNVERIFIED",
        reason="live order submitted but fill amount was not verified",
        raw_response=raw_response,
        order_id=str(order_id) if order_id is not None else None,
        details=details,
    )


def execute_market_order(
    *,
    config: dict[str, Any],
    token_id: str,
    amount_usd: float,
    side: str,
    preview_fn: Callable[..., dict[str, Any]],
    live_preflight_fn: Callable[..., PreflightDecision] = evaluate_live_funding_preflight,
    live_order_fn: Callable[..., dict[str, Any]] = submit_live_market_order,
    live_order_status_fn: Callable[[str], dict[str, Any]] = fetch_live_order_status,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> OrderExecutionResult:
    mode = resolve_execution_mode(config)

    if mode == "PREVIEW":
        preview = preview_fn(
            token_id=token_id,
            amount_usd=amount_usd,
            side=side,
        )
        return OrderExecutionResult(
            accepted=True,
            mode=mode,
            status="PREVIEW_READY",
            reason="preview order generated",
            raw_response=preview,
            fill_amount_usd=amount_usd,
        )

    if mode == "PAPER":
        response = {
            "token_id": token_id,
            "amount_usd": amount_usd,
            "side": side,
            "paper": True,
        }
        return OrderExecutionResult(
            accepted=True,
            mode=mode,
            status="PAPER_FILLED",
            reason="paper order simulated",
            raw_response=response,
            fill_amount_usd=amount_usd,
        )

    live_ok, live_reason = _live_enabled(config)
    if not live_ok:
        return OrderExecutionResult(
            accepted=False,
            mode="LIVE",
            status="LIVE_BLOCKED",
            reason=live_reason,
        )

    preflight = live_preflight_fn(
        config=config,
        side=side,
        amount_usd=amount_usd,
    )
    if not preflight.allowed:
        return OrderExecutionResult(
            accepted=False,
            mode="LIVE",
            status="LIVE_PREFLIGHT_BLOCKED",
            reason=preflight.reason,
            details=preflight.details,
        )

    live_cfg = config.get("live_execution", {})
    require_verified_fill = _bool_or_default(
        live_cfg.get("require_verified_fill"),
        True,
    )
    post_submit_poll_attempts = int(live_cfg.get("post_submit_poll_attempts", 3))
    post_submit_poll_interval_sec = float(
        live_cfg.get("post_submit_poll_interval_sec", 1.0)
    )

    try:
        raw_response = live_order_fn(
            token_id=token_id,
            amount_usd=amount_usd,
            side=side,
        )
    except Exception as e:
        return OrderExecutionResult(
            accepted=False,
            mode="LIVE",
            status="LIVE_SUBMIT_ERROR",
            reason=str(e),
        )

    parsed = parse_live_order_response(
        raw_response=raw_response,
        requested_amount_usd=amount_usd,
        require_verified_fill=require_verified_fill,
    )

    if parsed.status != "LIVE_SUBMITTED_UNVERIFIED" or not parsed.order_id:
        return parsed

    poll_history: list[dict[str, Any]] = []
    for attempt in range(max(post_submit_poll_attempts, 0)):
        if post_submit_poll_interval_sec > 0:
            sleep_fn(post_submit_poll_interval_sec)

        try:
            status_response = live_order_status_fn(parsed.order_id)
        except Exception as e:
            poll_history.append(
                {
                    "poll_attempt": attempt + 1,
                    "error": str(e),
                }
            )
            continue

        poll_history.append(
            {
                "poll_attempt": attempt + 1,
                "raw_response": status_response,
            }
        )
        status_parsed = parse_live_order_response(
            raw_response=status_response,
            requested_amount_usd=amount_usd,
            require_verified_fill=require_verified_fill,
        )
        if status_parsed.status != "LIVE_SUBMITTED_UNVERIFIED":
            merged_response = {
                "initial_submit_response": raw_response,
                "status_poll_history": poll_history,
                "final_status_response": status_response,
            }
            details = {
                **status_parsed.details,
                "post_submit_poll_attempts": len(poll_history),
            }
            return OrderExecutionResult(
                accepted=status_parsed.accepted,
                mode=status_parsed.mode,
                status=status_parsed.status,
                reason=status_parsed.reason,
                raw_response=merged_response,
                fill_amount_usd=status_parsed.fill_amount_usd,
                order_id=status_parsed.order_id or parsed.order_id,
                details=details,
            )

    merged_response = {
        "initial_submit_response": raw_response,
        "status_poll_history": poll_history,
    }
    details = {
        **parsed.details,
        "post_submit_poll_attempts": len(poll_history),
    }
    return OrderExecutionResult(
        accepted=False,
        mode=parsed.mode,
        status=parsed.status,
        reason="live order submitted but fill amount was not verified after post-submit polling",
        raw_response=merged_response,
        fill_amount_usd=0.0,
        order_id=parsed.order_id,
        details=details,
    )
