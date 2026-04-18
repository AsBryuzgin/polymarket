from __future__ import annotations

import json
import os
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class AlertDeliveryResult:
    channel: str
    attempted: bool
    delivered: bool
    reason: str


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


def _post_json(url: str, payload: dict[str, Any], *, timeout_sec: float) -> None:
    body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout_sec) as response:
        response.read()


def format_alert_message(alerts: list[dict[str, Any]], *, title: str = "Polymarket bot alert") -> str:
    if not alerts:
        return f"{title}: no alerts"

    lines = [title, f"alerts={len(alerts)}"]
    for idx, alert in enumerate(alerts[:10], start=1):
        severity = alert.get("severity", "UNKNOWN")
        alert_type = alert.get("alert_type", "UNKNOWN")
        message = alert.get("message", "")
        signal_id = alert.get("signal_id")
        suffix = f" | signal={signal_id}" if signal_id else ""
        lines.append(f"{idx}. [{severity}] {alert_type}: {message}{suffix}")
    if len(alerts) > 10:
        lines.append(f"... {len(alerts) - 10} more")
    return "\n".join(lines)


def deliver_alerts(
    *,
    config: dict[str, Any],
    alerts: list[dict[str, Any]],
    post_json: Callable[[str, dict[str, Any]], None] | None = None,
) -> list[AlertDeliveryResult]:
    delivery_cfg = config.get("alert_delivery", {})
    enabled = _bool_or_default(delivery_cfg.get("enabled"), False)
    timeout_sec = float(delivery_cfg.get("timeout_sec", 5.0))
    title = str(delivery_cfg.get("title") or "Polymarket bot alert")
    message = format_alert_message(alerts, title=title)

    if post_json is None:
        post_json = lambda url, payload: _post_json(url, payload, timeout_sec=timeout_sec)

    results: list[AlertDeliveryResult] = []
    if not enabled:
        return [
            AlertDeliveryResult(
                channel="all",
                attempted=False,
                delivered=False,
                reason="alert delivery disabled by config",
            )
        ]

    telegram_token = os.getenv(str(delivery_cfg.get("telegram_bot_token_env") or ""))
    telegram_chat_id = os.getenv(str(delivery_cfg.get("telegram_chat_id_env") or ""))
    if telegram_token and telegram_chat_id:
        url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
        payload = {"chat_id": telegram_chat_id, "text": message}
        results.append(_deliver_one("telegram", url, payload, post_json))

    for channel, env_key, payload in (
        (
            "discord",
            str(delivery_cfg.get("discord_webhook_url_env") or ""),
            {"content": message},
        ),
        (
            "email_webhook",
            str(delivery_cfg.get("email_webhook_url_env") or ""),
            {"subject": title, "text": message, "alerts": alerts},
        ),
        (
            "generic_webhook",
            str(delivery_cfg.get("generic_webhook_url_env") or ""),
            {"text": message, "alerts": alerts},
        ),
    ):
        url = os.getenv(env_key) if env_key else ""
        if url:
            results.append(_deliver_one(channel, url, payload, post_json))

    if not results:
        return [
            AlertDeliveryResult(
                channel="all",
                attempted=False,
                delivered=False,
                reason="no alert delivery destinations configured",
            )
        ]

    return results


def _deliver_one(
    channel: str,
    url: str,
    payload: dict[str, Any],
    post_json: Callable[[str, dict[str, Any]], None],
) -> AlertDeliveryResult:
    try:
        post_json(url, payload)
    except Exception as e:
        return AlertDeliveryResult(
            channel=channel,
            attempted=True,
            delivered=False,
            reason=str(e),
        )
    return AlertDeliveryResult(
        channel=channel,
        attempted=True,
        delivered=True,
        reason="ok",
    )
