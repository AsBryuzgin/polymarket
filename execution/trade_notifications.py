from __future__ import annotations

from typing import Any

from execution.alert_delivery import AlertDeliveryResult, deliver_text_notification


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


def _money(value: Any) -> str:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return "n/a"
    sign = "+" if parsed > 0 else ""
    return f"{sign}${parsed:.2f}"


def _price(value: Any) -> str:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return "n/a"
    return f"{parsed:.4f}"


def _pct(value: Any) -> str:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return "n/a"
    sign = "+" if parsed > 0 else ""
    return f"{sign}{parsed * 100:.2f}%"


def _short_token(token_id: str) -> str:
    token_id = str(token_id or "")
    if len(token_id) <= 18:
        return token_id
    return f"{token_id[:8]}...{token_id[-6:]}"


def _leader_label(user_name: str | None, wallet: str) -> str:
    if user_name:
        return str(user_name)
    wallet = str(wallet or "")
    if len(wallet) <= 14:
        return wallet
    return f"{wallet[:8]}...{wallet[-6:]}"


def format_trade_notification(
    *,
    mode: str,
    event_type: str,
    leader_wallet: str,
    leader_user_name: str | None,
    category: str | None,
    token_id: str,
    amount_usd: float,
    price: float | None,
    position_before_usd: float | None,
    position_after_usd: float | None,
    signal_id: str | None,
    realized_pnl_usd: float | None = None,
    realized_pnl_pct: float | None = None,
    holding_minutes: float | None = None,
    closed_fully: bool | None = None,
) -> str:
    event_type = event_type.upper()
    verb = "BUY" if event_type == "ENTRY" else "SELL"
    title = f"Polymarket {str(mode).upper()} {verb}"
    leader = _leader_label(leader_user_name, leader_wallet)
    category_text = category or "UNKNOWN"

    lines = [
        title,
        f"leader: {leader} | {category_text}",
        f"amount: {_money(amount_usd)} | price: {_price(price)}",
        f"position: {_money(position_before_usd)} -> {_money(position_after_usd)}",
        f"token: {_short_token(token_id)}",
    ]

    if event_type == "EXIT":
        exit_kind = "full exit" if closed_fully else "partial exit"
        lines.insert(2, f"type: {exit_kind}")
        lines.append(f"pnl: {_money(realized_pnl_usd)} ({_pct(realized_pnl_pct)})")
        if holding_minutes is not None:
            lines.append(f"holding: {float(holding_minutes):.1f} min")

    if signal_id:
        lines.append(f"signal: {signal_id}")

    return "\n".join(lines)


def send_trade_notification(
    *,
    config: dict[str, Any],
    mode: str,
    event_type: str,
    leader_wallet: str,
    leader_user_name: str | None,
    category: str | None,
    token_id: str,
    amount_usd: float,
    price: float | None,
    position_before_usd: float | None,
    position_after_usd: float | None,
    signal_id: str | None,
    realized_pnl_usd: float | None = None,
    realized_pnl_pct: float | None = None,
    holding_minutes: float | None = None,
    closed_fully: bool | None = None,
) -> list[AlertDeliveryResult]:
    delivery_cfg = config.get("alert_delivery", {})
    if not _bool_or_default(delivery_cfg.get("notify_trades"), True):
        return [
            AlertDeliveryResult(
                channel="all",
                attempted=False,
                delivered=False,
                reason="trade notifications disabled by config",
            )
        ]

    message = format_trade_notification(
        mode=mode,
        event_type=event_type,
        leader_wallet=leader_wallet,
        leader_user_name=leader_user_name,
        category=category,
        token_id=token_id,
        amount_usd=amount_usd,
        price=price,
        position_before_usd=position_before_usd,
        position_after_usd=position_after_usd,
        signal_id=signal_id,
        realized_pnl_usd=realized_pnl_usd,
        realized_pnl_pct=realized_pnl_pct,
        holding_minutes=holding_minutes,
        closed_fully=closed_fully,
    )

    return deliver_text_notification(
        config=config,
        message=message,
        title="Polymarket trade notification",
    )
