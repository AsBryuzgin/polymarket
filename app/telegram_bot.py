from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request
import uuid
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
    build_latency_report,
    build_leaders_report,
    build_positions_report,
    build_settlements_report,
    build_status_report,
    build_unmarked_report,
)
from execution.manual_unwind import (
    build_unwind_preview,
    execute_manual_unwind,
    format_unwind_result,
    list_unwind_targets,
)
from execution.order_router import resolve_execution_mode
from app.rebalance_review import (
    apply_manual_pick,
    apply_manual_replacement,
    approve_pending_review,
    build_review_message,
    create_rebalance_review,
    list_manual_candidates,
    load_pending_review,
    manual_candidate_categories,
    manual_candidates_for_category,
    reject_pending_review,
)


OFFSET_FILE = Path("data/telegram_bot_offset.json")
KEYBOARD = {
    "keyboard": [
        [{"text": "Статус"}, {"text": "Позиции"}],
        [{"text": "Лидеры"}, {"text": "Активность 24ч"}],
        [{"text": "Блокировки 24ч"}, {"text": "Неоцененные"}],
        [{"text": "Сеттлмент"}, {"text": "Latency"}],
        [{"text": "Ребаланс"}, {"text": "Выход"}],
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


def _post_multipart(
    url: str,
    *,
    fields: dict[str, str],
    file_field: str,
    file_path: Path,
    timeout_sec: float,
) -> dict[str, Any]:
    boundary = f"----polymarket-{uuid.uuid4().hex}"
    chunks: list[bytes] = []
    for name, value in fields.items():
        chunks.append(f"--{boundary}\r\n".encode("utf-8"))
        chunks.append(f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode("utf-8"))
        chunks.append(str(value).encode("utf-8"))
        chunks.append(b"\r\n")

    chunks.append(f"--{boundary}\r\n".encode("utf-8"))
    chunks.append(
        (
            f'Content-Disposition: form-data; name="{file_field}"; '
            f'filename="{file_path.name}"\r\n'
            "Content-Type: application/octet-stream\r\n\r\n"
        ).encode("utf-8")
    )
    chunks.append(file_path.read_bytes())
    chunks.append(b"\r\n")
    chunks.append(f"--{boundary}--\r\n".encode("utf-8"))

    body = b"".join(chunks)
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
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


def _money(value: Any) -> str:
    try:
        return f"${float(value):.2f}"
    except (TypeError, ValueError):
        return "$0.00"


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


def _send_document(
    *,
    token: str,
    chat_id: str,
    path: Path,
    timeout_sec: float,
    caption: str = "",
    reply_markup: dict[str, Any] | None = None,
) -> None:
    fields = {
        "chat_id": chat_id,
        "caption": caption[:900],
    }
    if reply_markup is not None:
        fields["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
    response = _post_multipart(
        _api_url(token, "sendDocument"),
        fields=fields,
        file_field="document",
        file_path=path,
        timeout_sec=timeout_sec,
    )
    if not response.get("ok"):
        raise RuntimeError(f"Telegram sendDocument failed: {response}")


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
        "allowed_updates": ["message", "callback_query"],
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
    if normalized in {"/unmarked", "unmarked", "неоцененные", "неоцененные позиции"}:
        return build_unmarked_report()
    if normalized in {"/settlements", "settlements", "/settlement", "settlement", "сеттлмент", "редим", "redeem"}:
        return build_settlements_report(config)
    if normalized in {"/latency", "latency", "задержка", "источники"}:
        return build_latency_report(config)
    return "Не понял команду. Нажми Помощь или отправь /help."


def _review_markup(review_id: str) -> dict[str, Any]:
    return {
        "inline_keyboard": [
            [
                {"text": "Подтвердить", "callback_data": f"rebalance_approve:{review_id}"},
                {"text": "Отменить", "callback_data": f"rebalance_reject:{review_id}"},
            ],
            [
                {"text": "Сменить кандидатов", "callback_data": f"rebalance_manual:{review_id}"},
            ],
        ]
    }


def _rebalance_replace_markup(review: dict[str, Any]) -> dict[str, Any]:
    review_id = str(review.get("review_id") or "")
    keyboard: list[list[dict[str, str]]] = []
    for idx, row in enumerate(review.get("proposed_live") or [], start=1):
        label = (
            f"{idx}. {row.get('user_name')} | {row.get('category')} | "
            f"WSS {row.get('final_wss')} | {float(row.get('weight') or 0.0) * 100:.2f}%"
        )
        keyboard.append(
            [
                {
                    "text": label[:58],
                    "callback_data": f"rebalance_replace:{review_id}:{idx}",
                }
            ]
        )
    keyboard.append(
        [
            {"text": "Назад", "callback_data": f"rebalance_back:{review_id}"},
            {"text": "Отменить", "callback_data": f"rebalance_reject:{review_id}"},
        ]
    )
    return {"inline_keyboard": keyboard}


def _build_rebalance_replace_text(review: dict[str, Any]) -> str:
    lines = [
        "Кого заменить?",
        f"review id: {review.get('review_id')}",
        "",
        "Сначала выбери текущего кандидата, которого убираем из proposed universe.",
    ]
    proposed = review.get("proposed_live") or []
    if proposed:
        lines.append("")
        lines.append("Текущий proposed universe:")
        for idx, row in enumerate(proposed, start=1):
            weight = float(row.get("weight") or 0.0) * 100.0
            lines.append(
                f"{idx}. {row.get('user_name')} | {row.get('category')} | "
                f"WSS {row.get('final_wss')} | {weight:.2f}%"
            )
    return "\n".join(lines)


def _rebalance_category_markup(review_id: str, replace_index: int) -> dict[str, Any]:
    keyboard: list[list[dict[str, str]]] = []
    row: list[dict[str, str]] = []
    for category in manual_candidate_categories():
        row.append(
            {
                "text": category,
                "callback_data": f"rebalance_category:{review_id}:{replace_index}:{category}",
            }
        )
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append(
        [
            {"text": "Назад", "callback_data": f"rebalance_manual:{review_id}"},
            {"text": "Отменить", "callback_data": f"rebalance_reject:{review_id}"},
        ]
    )
    return {"inline_keyboard": keyboard}


def _build_rebalance_category_text(review: dict[str, Any], replace_index: int) -> str:
    proposed = review.get("proposed_live") or []
    target = proposed[replace_index - 1] if 1 <= replace_index <= len(proposed) else {}
    lines = [
        "Из какой категории взять замену?",
        "",
        (
            f"Заменяем: {target.get('user_name', 'n/a')} | "
            f"{target.get('category', 'n/a')} | WSS {target.get('final_wss', 'n/a')}"
        ),
        "",
        "Можно выбрать любую категорию из свежего top-30.",
    ]
    return "\n".join(lines)


def _rebalance_candidate_markup(
    review_id: str,
    replace_index: int,
    category: str,
    *,
    limit: int = 10,
) -> dict[str, Any]:
    keyboard: list[list[dict[str, str]]] = []
    for idx, row in enumerate(manual_candidates_for_category(category, limit=limit), start=1):
        label = (
            f"{idx}. {row.get('user_name')} | WSS {row.get('final_wss')} | "
            f"copy {row.get('copyability_score')}"
        )
        keyboard.append(
            [
                {
                    "text": label[:58],
                    "callback_data": f"rebalance_pick_any:{review_id}:{replace_index}:{category}:{idx}",
                }
            ]
        )
    keyboard.append(
        [
            {"text": "Назад", "callback_data": f"rebalance_replace:{review_id}:{replace_index}"},
            {"text": "Отменить", "callback_data": f"rebalance_reject:{review_id}"},
        ]
    )
    return {"inline_keyboard": keyboard}


def _build_rebalance_candidate_text(
    review: dict[str, Any],
    replace_index: int,
    category: str,
    *,
    limit: int = 10,
) -> str:
    proposed = review.get("proposed_live") or []
    target = proposed[replace_index - 1] if 1 <= replace_index <= len(proposed) else {}
    rows = manual_candidates_for_category(category, limit=limit)
    if not rows:
        return f"Нет eligible кандидатов для {category.upper()}."
    lines = [
        f"Кем заменить {target.get('user_name', 'текущего кандидата')}?",
        f"Категория замены: {category.upper()}",
        "",
        "Нажми на кандидата:",
    ]
    for idx, row in enumerate(rows, start=1):
        lines.append(
            f"{idx}. {row.get('user_name')} | WSS {row.get('final_wss')} | "
            f"copy {row.get('copyability_score')} | "
            f"flow BUY/SELL {row.get('buy_trades_30d', '')}/{row.get('sell_trades_30d', '')} | "
            f"last {row.get('days_since_last_trade')}d"
        )
    return "\n".join(lines)


def _unwind_scope_label(scope: str) -> str:
    if scope == "ALL":
        return "все лидеры"
    for target in list_unwind_targets():
        if str(target.get("wallet") or "").lower() == scope.lower():
            return f"{target.get('user_name')} | {target.get('category') or 'UNKNOWN'}"
    return scope


def _unwind_selection_markup() -> dict[str, Any]:
    targets = list_unwind_targets()
    keyboard = [
        [
            {
                "text": "Все лидеры",
                "callback_data": "unwind_select:ALL",
            }
        ]
    ]
    for target in targets[:20]:
        wallet = str(target.get("wallet") or "")
        label = (
            f"{target.get('user_name')} | {target.get('category') or 'UNKNOWN'} | "
            f"{target.get('positions')} pos | {_money(target.get('position_usd'))}"
        )
        keyboard.append([{"text": label[:58], "callback_data": f"unwind_select:{wallet}"}])
    keyboard.append([{"text": "Отменить", "callback_data": "unwind_cancel:select"}])
    return {"inline_keyboard": keyboard}


def _build_unwind_selection_text(config: dict[str, Any]) -> str:
    targets = list_unwind_targets()
    total_positions = sum(int(row.get("positions") or 0) for row in targets)
    total_usd = sum(float(row.get("position_usd") or 0.0) for row in targets)
    mode = resolve_execution_mode(config)
    lines = [
        "Ручной выход по рынку",
        f"открытых позиций: {total_positions} | cost basis {_money(total_usd)}",
        f"режим: {mode}",
        "",
        "Выбери лидера или все позиции. Следующий экран попросит подтверждение.",
    ]
    if not targets:
        lines.append("Открытых позиций нет.")
    return "\n".join(lines)


def _unwind_confirm_markup(scope: str) -> dict[str, Any]:
    return {
        "inline_keyboard": [
            [
                {"text": "Подтвердить выход", "callback_data": f"unwind_confirm:{scope}"},
                {"text": "Отменить", "callback_data": "unwind_cancel:confirm"},
            ]
        ]
    }


def _build_unwind_confirm_text(scope: str) -> str:
    target_wallet = None if scope == "ALL" else scope
    preview = build_unwind_preview(target_wallet)
    leader_names = ", ".join(preview.get("leader_names") or [])
    lines = [
        "Подтвердить рыночный выход?",
        f"scope: {_unwind_scope_label(scope)}",
        f"лидеров: {preview.get('leaders')} | позиций: {preview.get('positions')} | cost basis {_money(preview.get('position_usd'))}",
    ]
    if leader_names:
        lines.append(f"лидеры: {leader_names}")
    lines.extend(
        [
            "",
            "После подтверждения бот отправит SELL FOK market order по каждой markable позиции.",
            "Resolved/no-orderbook позиции будут пропущены и останутся для settlement/redeem.",
        ]
    )
    return "\n".join(lines)


def _is_unwind_command(text: str) -> bool:
    return text.strip().lower() in {
        "/unwind",
        "unwind",
        "выход",
        "выйти",
        "закрыть позиции",
        "продать позиции",
    }


def _is_rebalance_command(text: str) -> bool:
    return text.strip().lower() in {
        "/rebalance",
        "rebalance",
        "ребаланс",
        "новый ребаланс",
        "review rebalance",
    }


def _handle_rebalance_command(
    *,
    token: str,
    chat_id: str,
    timeout_sec: float,
) -> None:
    _send_message(
        token=token,
        chat_id=chat_id,
        text=(
            "Пересобираю свежий top-30 по категориям и готовлю rebalance review. "
            "Это может занять несколько минут. Live universe пока не меняю."
        ),
        timeout_sec=timeout_sec,
        reply_markup=KEYBOARD,
    )
    review = create_rebalance_review()
    markup = _review_markup(str(review["review_id"]))
    _send_document(
        token=token,
        chat_id=chat_id,
        path=Path(review["files"]["all_csv"]),
        caption="Top-30 all categories CSV",
        timeout_sec=timeout_sec,
    )
    _send_document(
        token=token,
        chat_id=chat_id,
        path=Path(review["files"]["xlsx"]),
        caption="Top-30 all categories XLSX with WSS formulas",
        timeout_sec=timeout_sec,
    )
    _send_message(
        token=token,
        chat_id=chat_id,
        text=build_review_message(review),
        timeout_sec=timeout_sec,
        reply_markup=markup,
    )


def _handle_pick_command(text: str) -> tuple[str, dict[str, Any] | None] | None:
    parts = text.strip().split()
    if not parts:
        return None
    command = parts[0].lower()
    if command in {"candidates", "кандидаты"} and len(parts) == 2:
        return list_manual_candidates(parts[1]), None
    if command in {"pick", "выбрать"} and len(parts) == 3:
        result = apply_manual_pick(parts[1], int(parts[2]))
        review = result["review"]
        chosen = result["chosen"]
        replaced = result.get("replaced_category")
        replaced_text = f"\nЗаменена категория: {replaced}" if replaced else ""
        text = (
            f"Выбран {chosen.get('user_name')} | {chosen.get('category')} | "
            f"WSS {chosen.get('final_wss')}.{replaced_text}\n\n"
            f"{build_review_message(review)}\n\n"
            "Можно подтвердить, отменить или сменить кандидатов в других категориях."
        )
        return text, _review_markup(str(review["review_id"]))
    return None


def _answer_callback_query(
    *,
    token: str,
    callback_query_id: str,
    text: str,
    timeout_sec: float,
) -> None:
    _post_json(
        _api_url(token, "answerCallbackQuery"),
        {"callback_query_id": callback_query_id, "text": text[:180]},
        timeout_sec=timeout_sec,
    )


def _handle_callback_query(
    *,
    token: str,
    allowed_chat_id: str,
    callback_query: dict[str, Any],
    timeout_sec: float,
) -> None:
    callback_id = str(callback_query.get("id") or "")
    message = callback_query.get("message") or {}
    chat = message.get("chat") or {}
    chat_id = str(chat.get("id") or "")
    if chat_id != str(allowed_chat_id):
        return

    data = str(callback_query.get("data") or "")
    action, _, payload = data.partition(":")
    try:
        if action == "rebalance_approve":
            log = approve_pending_review(payload or None)
            _answer_callback_query(
                token=token,
                callback_query_id=callback_id,
                text="Rebalance approved",
                timeout_sec=timeout_sec,
            )
            _send_message(
                token=token,
                chat_id=allowed_chat_id,
                text=f"Rebalance {payload} применен.\n\n{log[-2500:]}",
                timeout_sec=timeout_sec,
                reply_markup=KEYBOARD,
            )
        elif action == "rebalance_reject":
            text = reject_pending_review(payload or None)
            _answer_callback_query(
                token=token,
                callback_query_id=callback_id,
                text="Rebalance rejected",
                timeout_sec=timeout_sec,
            )
            _send_message(
                token=token,
                chat_id=allowed_chat_id,
                text=text,
                timeout_sec=timeout_sec,
                reply_markup=KEYBOARD,
            )
        elif action == "rebalance_manual":
            review = load_pending_review()
            if not review:
                raise RuntimeError("no pending rebalance review")
            _answer_callback_query(
                token=token,
                callback_query_id=callback_id,
                text="Choose replacement slot",
                timeout_sec=timeout_sec,
            )
            _send_message(
                token=token,
                chat_id=allowed_chat_id,
                text=_build_rebalance_replace_text(review),
                timeout_sec=timeout_sec,
                reply_markup=_rebalance_replace_markup(review),
            )
        elif action == "rebalance_back":
            review = load_pending_review()
            if not review:
                raise RuntimeError("no pending rebalance review")
            _answer_callback_query(
                token=token,
                callback_query_id=callback_id,
                text="Back to review",
                timeout_sec=timeout_sec,
            )
            _send_message(
                token=token,
                chat_id=allowed_chat_id,
                text=build_review_message(review),
                timeout_sec=timeout_sec,
                reply_markup=_review_markup(str(review["review_id"])),
            )
        elif action == "rebalance_replace":
            review_id, replace_index_raw = payload.split(":", 1)
            review = load_pending_review()
            if not review:
                raise RuntimeError("no pending rebalance review")
            if review.get("review_id") != review_id:
                raise RuntimeError(f"pending review id mismatch: {review.get('review_id')} != {review_id}")
            replace_index = int(replace_index_raw)
            _answer_callback_query(
                token=token,
                callback_query_id=callback_id,
                text="Choose source category",
                timeout_sec=timeout_sec,
            )
            _send_message(
                token=token,
                chat_id=allowed_chat_id,
                text=_build_rebalance_category_text(review, replace_index),
                timeout_sec=timeout_sec,
                reply_markup=_rebalance_category_markup(review_id, replace_index),
            )
        elif action == "rebalance_category":
            review_id, replace_index_raw, category = payload.split(":", 2)
            review = load_pending_review()
            if not review:
                raise RuntimeError("no pending rebalance review")
            if review.get("review_id") != review_id:
                raise RuntimeError(f"pending review id mismatch: {review.get('review_id')} != {review_id}")
            replace_index = int(replace_index_raw)
            _answer_callback_query(
                token=token,
                callback_query_id=callback_id,
                text="Choose candidate",
                timeout_sec=timeout_sec,
            )
            _send_message(
                token=token,
                chat_id=allowed_chat_id,
                text=_build_rebalance_candidate_text(review, replace_index, category),
                timeout_sec=timeout_sec,
                reply_markup=_rebalance_candidate_markup(review_id, replace_index, category),
            )
        elif action == "rebalance_pick_any":
            review_id, replace_index_raw, category, pick_index_raw = payload.split(":", 3)
            result = apply_manual_replacement(
                replace_index=int(replace_index_raw),
                candidate_category=category,
                pick_index=int(pick_index_raw),
                review_id=review_id,
            )
            review = result["review"]
            chosen = result["chosen"]
            replaced = result["replaced"]
            _answer_callback_query(
                token=token,
                callback_query_id=callback_id,
                text="Candidate replaced",
                timeout_sec=timeout_sec,
            )
            text = (
                f"Заменил {replaced.get('user_name')} | {replaced.get('category')} "
                f"на {chosen.get('user_name')} | {chosen.get('category')} | "
                f"WSS {chosen.get('final_wss')}.\n\n"
                f"{build_review_message(review)}\n\n"
                "Можно подтвердить, отменить или сменить еще одного кандидата."
            )
            _send_message(
                token=token,
                chat_id=allowed_chat_id,
                text=text,
                timeout_sec=timeout_sec,
                reply_markup=_review_markup(str(review["review_id"])),
            )
        elif action == "unwind_select":
            scope = payload or "ALL"
            _answer_callback_query(
                token=token,
                callback_query_id=callback_id,
                text="Unwind target selected",
                timeout_sec=timeout_sec,
            )
            _send_message(
                token=token,
                chat_id=allowed_chat_id,
                text=_build_unwind_confirm_text(scope),
                timeout_sec=timeout_sec,
                reply_markup=_unwind_confirm_markup(scope),
            )
        elif action == "unwind_confirm":
            scope = payload or "ALL"
            target_wallet = None if scope == "ALL" else scope
            _answer_callback_query(
                token=token,
                callback_query_id=callback_id,
                text="Unwind started",
                timeout_sec=timeout_sec,
            )
            _send_message(
                token=token,
                chat_id=allowed_chat_id,
                text="Запускаю рыночный выход. Это может занять немного времени.",
                timeout_sec=timeout_sec,
                reply_markup=KEYBOARD,
            )
            summary = execute_manual_unwind(target_wallet=target_wallet)
            _send_message(
                token=token,
                chat_id=allowed_chat_id,
                text=format_unwind_result(summary),
                timeout_sec=timeout_sec,
                reply_markup=KEYBOARD,
            )
        elif action == "unwind_cancel":
            _answer_callback_query(
                token=token,
                callback_query_id=callback_id,
                text="Unwind cancelled",
                timeout_sec=timeout_sec,
            )
            _send_message(
                token=token,
                chat_id=allowed_chat_id,
                text="Ручной выход отменен.",
                timeout_sec=timeout_sec,
                reply_markup=KEYBOARD,
            )
        else:
            _answer_callback_query(
                token=token,
                callback_query_id=callback_id,
                text="Unknown action",
                timeout_sec=timeout_sec,
            )
    except Exception as e:
        _answer_callback_query(
            token=token,
            callback_query_id=callback_id,
            text="Command failed",
            timeout_sec=timeout_sec,
        )
        _send_message(
            token=token,
            chat_id=allowed_chat_id,
            text=f"Telegram callback failed: {e}",
            timeout_sec=timeout_sec,
            reply_markup=KEYBOARD,
        )


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

            callback_query = update.get("callback_query")
            if callback_query:
                _handle_callback_query(
                    token=token,
                    allowed_chat_id=allowed_chat_id,
                    callback_query=callback_query,
                    timeout_sec=timeout_sec,
                )
                continue

            message = update.get("message") or {}
            chat = message.get("chat") or {}
            chat_id = str(chat.get("id") or "")
            if chat_id != str(allowed_chat_id):
                continue

            text = str(message.get("text") or "").strip()
            if not text:
                continue

            try:
                if _is_rebalance_command(text):
                    _handle_rebalance_command(
                        token=token,
                        chat_id=allowed_chat_id,
                        timeout_sec=timeout_sec,
                    )
                    continue
                if _is_unwind_command(text):
                    _send_message(
                        token=token,
                        chat_id=allowed_chat_id,
                        text=_build_unwind_selection_text(config),
                        timeout_sec=timeout_sec,
                        reply_markup=_unwind_selection_markup(),
                    )
                    continue
                reply_markup = KEYBOARD
                pick_response = _handle_pick_command(text)
                if pick_response is not None:
                    response_text, custom_markup = pick_response
                    if custom_markup is not None:
                        reply_markup = custom_markup
                else:
                    response_text = _build_response(text, config)
            except Exception as e:
                response_text = f"Command failed: {e}"
                reply_markup = KEYBOARD

            _send_message(
                token=token,
                chat_id=allowed_chat_id,
                text=response_text,
                timeout_sec=timeout_sec,
                reply_markup=reply_markup,
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
