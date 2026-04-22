from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.builder_auth import load_executor_config
from execution.telegram_reports import (
    build_activity_report,
    build_blocks_report,
    build_help_report,
    build_leaders_report,
    build_positions_report,
    build_status_report,
)


OFFSET_FILE = Path("data/telegram_bot_offset.json")
KEYBOARD = {
    "keyboard": [
        [{"text": "Статус"}, {"text": "Позиции"}],
        [{"text": "Лидеры"}, {"text": "Активность 24ч"}],
        [{"text": "Блокировки 24ч"}],
        [{"text": "Помощь"}],
    ],
    "resize_keyboard": True,
    "one_time_keyboard": False,
}


def _post_json(url: str, payload: dict[str, Any], *, timeout_sec: float) -> dict[str, Any]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout_sec) as response:
        raw = response.read().decode("utf-8")
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise RuntimeError(f"unexpected Telegram response: {parsed!r}")
    return parsed


def _telegram_env(config: dict[str, Any]) -> tuple[str, str]:
    delivery_cfg = config.get("alert_delivery", {})
    token_env = str(delivery_cfg.get("telegram_bot_token_env") or "POLY_ALERT_TELEGRAM_BOT_TOKEN")
    chat_env = str(delivery_cfg.get("telegram_chat_id_env") or "POLY_ALERT_TELEGRAM_CHAT_ID")
    token = os.getenv(token_env, "").strip()
    chat_id = os.getenv(chat_env, "").strip()
    if not token:
        raise RuntimeError(f"missing Telegram bot token env: {token_env}")
    if not chat_id:
        raise RuntimeError(f"missing Telegram chat id env: {chat_env}")
    return token, chat_id


def _api_url(token: str, method: str) -> str:
    return f"https://api.telegram.org/bot{token}/{method}"


def _load_offset(path: Path = OFFSET_FILE) -> int | None:
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    try:
        return int(raw.get("offset"))
    except Exception:
        return None


def _save_offset(offset: int, path: Path = OFFSET_FILE) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"offset": offset}, indent=2), encoding="utf-8")


def _send_message(
    *,
    token: str,
    chat_id: str,
    text: str,
    timeout_sec: float,
    reply_markup: dict[str, Any] | None = None,
) -> None:
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "text": text[:3900],
    }
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    _post_json(_api_url(token, "sendMessage"), payload, timeout_sec=timeout_sec)


def _get_updates(
    *,
    token: str,
    offset: int | None,
    poll_timeout_sec: int,
    request_timeout_sec: float,
    limit: int = 20,
) -> list[dict[str, Any]]:
    payload: dict[str, Any] = {
        "timeout": poll_timeout_sec,
        "limit": limit,
        "allowed_updates": ["message"],
    }
    if offset is not None:
        payload["offset"] = offset
    response = _post_json(_api_url(token, "getUpdates"), payload, timeout_sec=request_timeout_sec)
    if not response.get("ok"):
        raise RuntimeError(f"Telegram getUpdates failed: {response}")
    result = response.get("result") or []
    return result if isinstance(result, list) else []


def _prime_offset(
    *,
    token: str,
    timeout_sec: float,
) -> int | None:
    response = _post_json(
        _api_url(token, "getUpdates"),
        {"timeout": 0, "limit": 1, "offset": -1, "allowed_updates": ["message"]},
        timeout_sec=timeout_sec,
    )
    result = response.get("result") or []
    if not isinstance(result, list) or not result:
        return None
    try:
        return int(result[-1]["update_id"]) + 1
    except Exception:
        return None


def _build_response(text: str, config: dict[str, Any]) -> str:
    normalized = text.strip().lower()
    if normalized in {"/start", "start", "/help", "help", "помощь"}:
        return build_help_report()
    if normalized in {"/status", "status", "статус", "баланс", "balance"}:
        return build_status_report(config)
    if normalized in {"/positions", "positions", "позиции"}:
        return build_positions_report()
    if normalized in {"/leaders", "leaders", "лидеры"}:
        return build_leaders_report()
    if normalized in {"/activity", "activity", "activity 24h", "активность", "активность 24ч"}:
        return build_activity_report()
    if normalized in {"/blocks", "blocks", "blocks 24h", "блоки", "блокировки", "блокировки 24ч"}:
        return build_blocks_report()
    return "Не понял команду. Нажми Помощь или отправь /help."


def run_bot(*, poll_sec: float, timeout_sec: float, process_pending: bool) -> None:
    config = load_executor_config()
    token, allowed_chat_id = _telegram_env(config)
    offset = _load_offset()

    if offset is None and not process_pending:
        offset = _prime_offset(token=token, timeout_sec=timeout_sec)
        if offset is not None:
            _save_offset(offset)

    print("=== TELEGRAM BOT ===")
    print({"poll_sec": poll_sec, "process_pending": process_pending, "offset": offset})

    while True:
        updates = _get_updates(
            token=token,
            offset=offset,
            poll_timeout_sec=max(int(poll_sec), 1),
            request_timeout_sec=timeout_sec + max(float(poll_sec), 1.0),
        )

        for update in updates:
            update_id = int(update.get("update_id"))
            offset = update_id + 1
            _save_offset(offset)

            message = update.get("message") or {}
            chat = message.get("chat") or {}
            chat_id = str(chat.get("id") or "")
            if chat_id != str(allowed_chat_id):
                continue

            text = str(message.get("text") or "").strip()
            if not text:
                continue

            try:
                response_text = _build_response(text, config)
            except Exception as e:
                response_text = f"Command failed: {e}"

            _send_message(
                token=token,
                chat_id=allowed_chat_id,
                text=response_text,
                timeout_sec=timeout_sec,
                reply_markup=KEYBOARD,
            )

        if not updates:
            time.sleep(max(float(poll_sec), 0.5))


def main() -> None:
    parser = argparse.ArgumentParser(description="Telegram control bot for Polymarket executor.")
    parser.add_argument("--poll-sec", type=float, default=2.0)
    parser.add_argument("--timeout-sec", type=float, default=10.0)
    parser.add_argument(
        "--process-pending",
        action="store_true",
        help="Process Telegram messages that arrived before the first bot start.",
    )
    args = parser.parse_args()

    run_bot(
        poll_sec=args.poll_sec,
        timeout_sec=args.timeout_sec,
        process_pending=args.process_pending,
    )


if __name__ == "__main__":
    main()
