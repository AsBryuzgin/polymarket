from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from execution.allowance import fetch_collateral_balance_allowance
from execution.market_cache import market_cache_summary
from execution.market_diagnostics import diagnose_market_snapshot_error
from execution.onchain_shadow import onchain_shadow_summary
from execution.position_marking import is_marked, is_unmarked, mark_position
from execution.polymarket_executor import fetch_market_snapshot
from execution.settlement import build_settlement_report
from execution.signal_observation_store import (
    init_signal_observation_table,
    list_signal_observations,
)
from execution.state_store import (
    init_db,
    list_leader_registry,
    list_micro_signal_buckets,
    list_open_positions,
    list_processed_signals,
    list_trade_history,
)
from risk.adaptive_sizing import compute_adaptive_sizing_decision


SnapshotLoader = Callable[[str, str], dict[str, Any]]


def _safe_float(value: Any) -> float:
    try:
        return float(value) if value is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _maybe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        raw = str(value).strip()
        if not raw:
            return None
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00").replace(" ", "T"))
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _money(value: Any, *, signed: bool = False) -> str:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return "n/a"
    sign = "+" if signed and parsed > 0 else ""
    return f"{sign}${parsed:.2f}"


def _pct(value: Any) -> str:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return "n/a"
    return f"{parsed * 100:.1f}%"


def _num(value: Any, digits: int = 4) -> str:
    parsed = _maybe_float(value)
    if parsed is None:
        return "n/a"
    return f"{parsed:.{digits}f}"


def _age(value: Any) -> str:
    seconds = _maybe_float(value)
    if seconds is None:
        return "n/a"
    seconds = max(seconds, 0.0)
    if seconds < 90:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        return f"{seconds / 60.0:.1f}m"
    if seconds < 86400:
        return f"{seconds / 3600.0:.1f}h"
    return f"{seconds / 86400.0:.1f}d"


def _status_hint(status: str) -> str:
    hints = {
        "FRESH_COPYABLE": "свежий сигнал, можно копировать",
        "LATE_BUT_COPYABLE": "сигнал старше свежего окна, но еще допустим",
        "EXIT_FOLLOW": "выход по уже открытой позиции",
        "EXIT_FOLLOW_STALE": "запаздывающий выход по открытой позиции",
        "POLICY_BLOCKED": "остановлен фильтром цены, spread, ликвидности или размера",
        "DRIFT_BLOCKED": "текущая цена слишком ушла от цены входа лидера",
        "SKIPPED_NO_POSITION": "лидер продает, но у бота нет такой позиции",
        "ALREADY_PROCESSED": "этот signal уже обработан раньше",
        "EXIT_ONLY_BUY_BLOCKED": "лидер в EXIT_ONLY, новые входы запрещены",
        "NO_ORDERBOOK": "нет стакана по токену",
        "NO_SIGNAL": "нет выбранного сигнала",
        "TOO_OLD": "BUY слишком старый для нового входа",
        "ACCUMULATED_PENDING": "маленький BUY сохранен и ждет накопления до min order",
        "ACCUMULATED_EXECUTED": "микросигнал был исполнен внутри общего BUY",
        "ACCUMULATED_EXPIRED": "микросигнал устарел до накопления min order",
        "ACCUMULATED_EXECUTION_ERROR": "общий BUY по накопленным микросигналам не исполнился",
        "BATCH_PENDING": "маленький BUY ждет короткой склейки с соседними fill'ами",
        "BATCH_EXECUTED": "маленький BUY был исполнен внутри короткого batch",
        "BATCH_EXPIRED": "короткое batch-окно закончилось до min order",
        "BATCH_BLOCKED": "короткий batch собрался, но был остановлен risk/budget",
        "BATCH_EXECUTION_ERROR": "короткий batch не исполнился",
    }
    return hints.get(status, "см. latest_reason для деталей")


def _age_minutes(dt: datetime | None, *, now: datetime) -> float | None:
    if dt is None:
        return None
    return max((now - dt).total_seconds() / 60.0, 0.0)


def _short(value: str, left: int = 8, right: int = 6) -> str:
    value = str(value or "")
    if len(value) <= left + right + 3:
        return value
    if right <= 0:
        return f"{value[:left]}..."
    return f"{value[:left]}...{value[-right:]}"


def _leader_name(row: dict[str, Any]) -> str:
    return str(row.get("leader_user_name") or row.get("user_name") or _short(row.get("leader_wallet") or row.get("wallet") or "UNKNOWN"))


def _unique_count(rows: list[dict[str, Any]], key: str) -> int:
    return len({str(row.get(key) or "") for row in rows if row.get(key)})


def _drift_abs(reason: Any) -> float | None:
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)\\s*>", str(reason or ""))
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def _reason_bucket(row: dict[str, Any]) -> str:
    status = str(row.get("latest_status") or "UNKNOWN")
    reason = str(row.get("latest_reason") or "").lower()
    if status == "DRIFT_BLOCKED":
        return "price drift"
    if "spread" in reason:
        return "spread"
    if "below min_price" in reason:
        return "price too low"
    if "above max_price" in reason:
        return "price too high"
    if "min order" in reason or "min_order" in reason or "budget below" in reason:
        return "size/min order"
    if "liquidity" in reason or "liquid" in reason:
        return "liquidity"
    return status.lower()


def _latest_snapshot_value(row: dict[str, Any], name: str) -> Any:
    latest_value = row.get(f"latest_snapshot_{name}")
    if _maybe_float(latest_value) is not None:
        return latest_value
    if row.get("selected_signal_id"):
        return None
    return row.get(f"snapshot_{name}")


def _reason_price_values(reason: Any) -> dict[str, float | None]:
    text = str(reason or "")
    values: dict[str, float | None] = {
        "midpoint": None,
        "spread": None,
        "spread_rel": None,
    }

    spread_match = re.search(
        r"spread\s+([0-9]+(?:\.[0-9]+)?)\s+\(([0-9]+(?:\.[0-9]+)?)%\s+of midpoint\)",
        text,
        flags=re.IGNORECASE,
    )
    if spread_match:
        spread = _maybe_float(spread_match.group(1))
        spread_rel = (_maybe_float(spread_match.group(2)) or 0.0) / 100.0
        values["spread"] = spread
        values["spread_rel"] = spread_rel if spread_rel > 0 else None
        if spread is not None and spread_rel > 0:
            values["midpoint"] = spread / spread_rel

    midpoint_match = re.search(
        r"midpoint\s+([0-9]+(?:\.[0-9]+)?)\s+(?:above|below)",
        text,
        flags=re.IGNORECASE,
    )
    if midpoint_match:
        values["midpoint"] = _maybe_float(midpoint_match.group(1))

    return values


def _block_key(row: dict[str, Any]) -> str:
    latest_hash = str(row.get("latest_trade_hash") or "").strip()
    if latest_hash:
        return latest_hash
    return "|".join(
        [
            str(row.get("leader_wallet") or ""),
            str(row.get("latest_status") or ""),
            str(row.get("latest_reason") or ""),
            str(row.get("latest_token_id") or row.get("token_id") or ""),
            str(row.get("latest_trade_side") or ""),
        ]
    )


def _summarize_block(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(
        rows,
        key=lambda row: _safe_dt(row.get("observed_at")) or datetime.min.replace(tzinfo=timezone.utc),
    )
    first = ordered[0]
    last = ordered[-1]
    ages = [_maybe_float(row.get("latest_trade_age_sec")) for row in ordered]
    ages = [age for age in ages if age is not None]
    midpoint = _latest_snapshot_value(last, "midpoint")
    spread = _latest_snapshot_value(last, "spread")
    midpoint_num = _maybe_float(midpoint)
    spread_num = _maybe_float(spread)
    reason_values = _reason_price_values(last.get("latest_reason"))
    if midpoint_num is None:
        midpoint_num = reason_values.get("midpoint")
        midpoint = midpoint_num
    if spread_num is None:
        spread_num = reason_values.get("spread")
        spread = spread_num
    spread_rel = None
    if midpoint_num and spread_num is not None:
        spread_rel = spread_num / midpoint_num
    if spread_rel is None:
        spread_rel = reason_values.get("spread_rel")
    return {
        "leader": _leader_name(last),
        "category": str(last.get("category") or "UNKNOWN"),
        "status": str(last.get("latest_status") or "UNKNOWN"),
        "reason": str(last.get("latest_reason") or "UNKNOWN"),
        "reason_bucket": _reason_bucket(last),
        "checks": len(rows),
        "side": str(last.get("latest_trade_side") or "n/a"),
        "hash": str(last.get("latest_trade_hash") or ""),
        "token": str(last.get("latest_token_id") or last.get("token_id") or ""),
        "leader_price": last.get("latest_trade_price"),
        "midpoint": midpoint,
        "spread": spread,
        "spread_rel": spread_rel,
        "age_first": min(ages) if ages else None,
        "age_last": max(ages) if ages else None,
        "observed_at": _safe_dt(last.get("observed_at")),
    }


def _load_latest_alert_count(path: Path = Path("data/executor_alerts_latest.json")) -> int | None:
    if not path.exists():
        return None
    try:
        import json

        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return len(raw) if isinstance(raw, list) else None


def _open_position_marks(
    *,
    snapshot_loader: SnapshotLoader = fetch_market_snapshot,
) -> tuple[list[dict[str, Any]], list[str]]:
    rows = []
    errors = []

    for pos in list_open_positions(limit=100000):
        row = mark_position(
            pos,
            snapshot_loader=snapshot_loader,
            diagnosis_loader=diagnose_market_snapshot_error,
            snapshot_side="SELL",
        )
        if is_unmarked(row):
            errors.append(f"{_short(str(pos.get('token_id')))}: {row.get('snapshot_reason')}")
        rows.append(row)

    return rows, errors


def _funding_snapshot(config: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    try:
        return asdict(fetch_collateral_balance_allowance(config)), None
    except Exception as e:
        return None, str(e)


def _configured_capital_usd(config: dict[str, Any]) -> float | None:
    capital = _safe_float(config.get("capital", {}).get("total_capital_usd"))
    return capital if capital > 0 else None


def _row_mark_value(row: dict[str, Any], field: str) -> float:
    parsed = _maybe_float(row.get(field))
    return parsed if parsed is not None else 0.0


def _row_open_pnl(row: dict[str, Any], field: str) -> float:
    return _row_mark_value(row, field) - _safe_float(row.get("position_usd"))


def _has_mid_but_no_bid(row: dict[str, Any]) -> bool:
    return (
        is_marked(row)
        and _maybe_float(row.get("mark_value_mid_usd")) is not None
        and _maybe_float(row.get("mark_value_bid_usd")) is None
    )


def _open_mark_summary(open_rows: list[dict[str, Any]]) -> dict[str, Any]:
    invested = sum(_safe_float(row.get("position_usd")) for row in open_rows)
    mark_bid_liquidation = sum(_row_mark_value(row, "mark_value_bid_usd") for row in open_rows)
    mark_mid = sum(_row_mark_value(row, "mark_value_mid_usd") for row in open_rows)
    no_bid_rows = [row for row in open_rows if _has_mid_but_no_bid(row)]
    unmarked_rows = [row for row in open_rows if is_unmarked(row)]
    settled_rows = [row for row in open_rows if str(row.get("snapshot_status")) == "SETTLED"]

    return {
        "invested": invested,
        "mark_bid_liquidation": mark_bid_liquidation,
        "mark_mid": mark_mid,
        "pnl_bid_liquidation": mark_bid_liquidation - invested,
        "pnl_mid": mark_mid - invested,
        "no_bid_rows": no_bid_rows,
        "no_bid_invested": sum(_safe_float(row.get("position_usd")) for row in no_bid_rows),
        "unmarked_rows": unmarked_rows,
        "unmarked_invested": sum(_safe_float(row.get("position_usd")) for row in unmarked_rows),
        "settled_rows": settled_rows,
    }


def _realized_pnl_from_history() -> float:
    return sum(
        _safe_float(row.get("realized_pnl_usd"))
        for row in list_trade_history(limit=100000)
        if row.get("event_type") == "EXIT"
    )


def build_status_report(
    config: dict[str, Any],
    *,
    now: datetime | None = None,
    snapshot_loader: SnapshotLoader = fetch_market_snapshot,
) -> str:
    init_db()
    init_signal_observation_table()
    now = now or datetime.now(timezone.utc)

    open_rows, snapshot_errors = _open_position_marks(snapshot_loader=snapshot_loader)
    registry = list_leader_registry(limit=100000)
    signal_batches = list_micro_signal_buckets(limit=100000)
    observations = list_signal_observations(limit=1)

    mark_summary = _open_mark_summary(open_rows)
    invested = mark_summary["invested"]
    settled_rows = mark_summary["settled_rows"]
    unmarked_rows = mark_summary["unmarked_rows"]
    unmarked_invested = mark_summary["unmarked_invested"]
    no_bid_rows = mark_summary["no_bid_rows"]

    mode = str(config.get("global", {}).get("execution_mode", "unknown")).lower()
    paper_bankroll = _configured_capital_usd(config) if mode == "paper" else None
    funding = None
    funding_error = None

    if paper_bankroll is not None:
        paper_realized_pnl = _realized_pnl_from_history()
        cash = max(paper_bankroll + paper_realized_pnl - invested, 0.0)
        allowance = None
    else:
        paper_realized_pnl = None
        funding, funding_error = _funding_snapshot(config)
        cash = funding.get("balance_usd") if funding else None
        allowance = funding.get("allowance_usd") if funding else None

    total_bid = cash + mark_summary["mark_bid_liquidation"] if cash is not None else None
    total_mid = cash + mark_summary["mark_mid"] if cash is not None else None

    active_leaders = sum(1 for row in registry if row.get("leader_status") == "ACTIVE")
    exit_only_leaders = sum(1 for row in registry if row.get("leader_status") == "EXIT_ONLY")
    last_observed_at = _safe_dt(observations[0].get("observed_at")) if observations else None
    last_age = _age_minutes(last_observed_at, now=now)
    alert_count = _load_latest_alert_count()

    lines = [
        "Статус Polymarket bot",
        f"Режим: {mode.upper()}",
        "",
        "Баланс",
    ]
    if paper_bankroll is not None:
        lines.append(f"банкролл paper: {_money(paper_bankroll)}")
        lines.append(f"realized PnL paper: {_money(paper_realized_pnl, signed=True)}")
    lines.extend(
        [
            f"свободно без открытых позиций: {_money(cash)}",
            f"allowance: {_money(allowance)}",
            "",
            "Портфель",
            f"открытых позиций: {len(open_rows)} | вложено: {_money(invested)}",
            f"equity по bid: {_money(total_bid)} | по mid: {_money(total_mid)}",
            (
                "open PnL по bid/mid: "
                f"{_money(mark_summary['pnl_bid_liquidation'], signed=True)} / "
                f"{_money(mark_summary['pnl_mid'], signed=True)}"
            ),
            "",
            "Система",
            f"лидеры: {active_leaders} active, {exit_only_leaders} exit-only",
        ]
    )

    if last_age is not None:
        lines.append(f"последнее наблюдение: {last_age:.1f} мин назад")
    if alert_count is not None:
        lines.append(f"текущие alerts: {alert_count}")
    if signal_batches:
        batch_amount = sum(_safe_float(row.get("pending_amount_usd")) for row in signal_batches)
        batch_signals = sum(int(_safe_float(row.get("signal_count"))) for row in signal_batches)
        lines.append(
            f"short batch: {len(signal_batches)} buckets | "
            f"{batch_signals} signals | {_money(batch_amount)}"
        )
    if funding_error:
        lines.append(f"проверка баланса: ERROR {_short(funding_error, 40, 0)}")
    if snapshot_errors:
        lines.append(
            f"неоцененные по рынку: {len(snapshot_errors)} | "
            f"сумма: {_money(unmarked_invested)}"
        )
        lines.append("детали по неоцененным: /unmarked")
    if no_bid_rows:
        lines.append(
            f"нет bid в стакане: {len(no_bid_rows)} | "
            f"сумма: {_money(mark_summary['no_bid_invested'])} | bid считает как $0"
        )
    if settled_rows:
        lines.append(
            f"settlement-marked: {len(settled_rows)} | "
            f"сумма: {_money(sum(_safe_float(row.get('position_usd')) for row in settled_rows))}"
        )

    return "\n".join(lines)


def build_positions_report(
    *,
    snapshot_loader: SnapshotLoader = fetch_market_snapshot,
) -> str:
    init_db()
    open_rows, snapshot_errors = _open_position_marks(snapshot_loader=snapshot_loader)

    if not open_rows:
        return "Открытые позиции\nпозиций нет"

    mark_summary = _open_mark_summary(open_rows)
    invested = mark_summary["invested"]
    settled_rows = mark_summary["settled_rows"]
    unmarked_rows = mark_summary["unmarked_rows"]
    unmarked_invested = mark_summary["unmarked_invested"]
    no_bid_rows = mark_summary["no_bid_rows"]
    lines = [
        "Открытые позиции",
        (
            f"Всего: {len(open_rows)} | вложено: {_money(invested)} | "
            f"bid PnL: {_money(mark_summary['pnl_bid_liquidation'], signed=True)}"
        ),
        "",
    ]
    registry = {row["wallet"]: row for row in list_leader_registry(limit=100000)}
    sorted_rows = sorted(open_rows, key=lambda x: _safe_float(x.get("position_usd")), reverse=True)
    for idx, row in enumerate(sorted_rows[:12], start=1):
        leader = registry.get(row.get("leader_wallet"), {})
        name = _leader_name({**leader, **row})
        category = leader.get("category") or "UNKNOWN"
        lines.extend(
            [
                f"{idx}. {name} | {category}",
                (
                    f"   вход {_money(row.get('position_usd'))} -> bid "
                    f"{_money(row.get('mark_value_bid_usd'))} | PnL "
                    f"{_money(_row_open_pnl(row, 'mark_value_bid_usd'), signed=True)}"
                ),
                f"   token {_short(str(row.get('token_id')))}",
            ]
        )
        if _has_mid_but_no_bid(row):
            lines.append("   bid отсутствует: ликвидационная оценка считает позицию как $0")
        if row.get("snapshot_status") == "SETTLED":
            lines.append(
                "   оценка: settlement fallback "
                f"по resolved market, price={_num(row.get('settlement_price'))}"
            )
        elif row.get("snapshot_status") != "OK":
            lines.append("   market snapshot: ERROR, позиция не оценена по рынку")

    if len(open_rows) > 12:
        lines.append(f"... еще {len(open_rows) - 12}")
    if snapshot_errors:
        lines.append(f"неоцененные по рынку: {len(snapshot_errors)} | сумма: {_money(unmarked_invested)}")
        lines.append("подробно: /unmarked")
    if no_bid_rows:
        lines.append(f"нет bid в стакане: {len(no_bid_rows)} | сумма: {_money(mark_summary['no_bid_invested'])}")
    if settled_rows:
        lines.append(
            f"settlement-marked: {len(settled_rows)} | "
            f"сумма: {_money(sum(_safe_float(row.get('position_usd')) for row in settled_rows))}"
        )

    return "\n".join(lines)


def build_unmarked_report(
    *,
    snapshot_loader: SnapshotLoader = fetch_market_snapshot,
) -> str:
    init_db()
    open_rows, _snapshot_errors = _open_position_marks(snapshot_loader=snapshot_loader)
    unmarked_rows = [row for row in open_rows if is_unmarked(row)]

    if not unmarked_rows:
        return "Неоцененные позиции\nнеоцененных позиций нет"

    registry = {row["wallet"]: row for row in list_leader_registry(limit=100000)}
    invested = sum(_safe_float(row.get("position_usd")) for row in unmarked_rows)
    diagnoses: dict[str, dict[str, Any]] = {}
    lines = [
        "Неоцененные позиции",
        f"Всего: {len(unmarked_rows)} | вложено: {_money(invested)}",
        "Причина: по этим токенам CLOB не дал стакан, поэтому mark-to-market сейчас недоступен.",
        "",
    ]

    sorted_rows = sorted(unmarked_rows, key=lambda row: _safe_float(row.get("position_usd")), reverse=True)
    for idx, row in enumerate(sorted_rows[:12], start=1):
        token_id = str(row.get("token_id") or "")
        diagnosis = diagnoses.get(token_id)
        if diagnosis is None:
            diagnosis = diagnose_market_snapshot_error(token_id, str(row.get("snapshot_error") or ""))
            diagnoses[token_id] = diagnosis

        leader = registry.get(row.get("leader_wallet"), {})
        name = _leader_name({**leader, **row})
        category = leader.get("category") or "UNKNOWN"
        flags = []
        for key, label in (
            ("active", "active"),
            ("closed", "closed"),
            ("archived", "archived"),
            ("accepting_orders", "accepting"),
            ("enable_order_book", "orderbook"),
        ):
            value = diagnosis.get(key)
            if value is not None:
                flags.append(f"{label}={value}")
        if diagnosis.get("uma_resolution_status"):
            flags.append(f"uma={diagnosis['uma_resolution_status']}")

        lines.extend(
            [
                f"{idx}. {name} | {category} | {_money(row.get('position_usd'))}",
                f"   token {_short(token_id)}",
                f"   диагноз: {diagnosis.get('diagnosis_label')} | {diagnosis.get('diagnosis_reason')}",
            ]
        )
        if diagnosis.get("question"):
            lines.append(f"   market: {diagnosis['question']}")
        if diagnosis.get("token_outcome"):
            outcome_line = f"   outcome: {diagnosis['token_outcome']}"
            if diagnosis.get("token_winner") is not None:
                outcome_line += f" | winner={diagnosis['token_winner']}"
            lines.append(outcome_line)
        if flags:
            lines.append(f"   flags: {' | '.join(flags)}")
        if diagnosis.get("action_hint"):
            lines.append(f"   action: {diagnosis['action_hint']}")
        if row.get("snapshot_error"):
            lines.append(f"   snapshot: {row['snapshot_error']}")

    if len(unmarked_rows) > 12:
        lines.append(f"... еще {len(unmarked_rows) - 12}")

    status_counts = Counter(str(diagnoses[str(row.get("token_id") or "")].get("diagnosis_status") or "UNKNOWN") for row in unmarked_rows)
    if status_counts:
        lines.extend(
            [
                "",
                "Итог по диагнозам:",
                ", ".join(f"{status}={count}" for status, count in sorted(status_counts.items())),
            ]
        )

    return "\n".join(lines)


def build_settlements_report(
    config: dict[str, Any],
    *,
    snapshot_loader: SnapshotLoader = fetch_market_snapshot,
) -> str:
    init_db()
    return build_settlement_report(
        config=config,
        snapshot_loader=snapshot_loader,
    )


def build_leaders_report(
    *,
    snapshot_loader: SnapshotLoader = fetch_market_snapshot,
) -> str:
    init_db()
    history = list_trade_history(limit=100000)
    open_rows, _snapshot_errors = _open_position_marks(snapshot_loader=snapshot_loader)
    registry = {row["wallet"]: row for row in list_leader_registry(limit=100000)}
    grouped: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "entries": 0,
            "exits": 0,
            "realized_pnl_usd": 0.0,
            "unrealized_pnl_bid_usd": 0.0,
            "unrealized_pnl_mid_usd": 0.0,
            "invested_open_usd": 0.0,
            "no_bid_invested_usd": 0.0,
            "category": "UNKNOWN",
            "name": "UNKNOWN",
        }
    )

    for row in history:
        wallet = str(row.get("leader_wallet") or "")
        item = grouped[wallet]
        item["name"] = _leader_name(row)
        item["category"] = row.get("category") or item["category"]
        if row.get("event_type") == "ENTRY":
            item["entries"] += 1
        if row.get("event_type") == "EXIT":
            item["exits"] += 1
            item["realized_pnl_usd"] += _safe_float(row.get("realized_pnl_usd"))

    for row in open_rows:
        wallet = str(row.get("leader_wallet") or "")
        item = grouped[wallet]
        leader = registry.get(wallet, {})
        item["name"] = _leader_name({**leader, **row})
        item["category"] = leader.get("category") or item["category"]
        item["unrealized_pnl_bid_usd"] += _row_open_pnl(row, "mark_value_bid_usd")
        item["unrealized_pnl_mid_usd"] += _row_open_pnl(row, "mark_value_mid_usd")
        item["invested_open_usd"] += _safe_float(row.get("position_usd"))
        if _has_mid_but_no_bid(row):
            item["no_bid_invested_usd"] += _safe_float(row.get("position_usd"))

    active_wallets = {
        str(row.get("wallet"))
        for row in registry.values()
        if row.get("leader_status") == "ACTIVE"
    }
    for wallet in active_wallets:
        leader = registry.get(wallet, {})
        item = grouped[wallet]
        item["name"] = _leader_name(leader)
        item["category"] = leader.get("category") or item["category"]

    rows = []
    for wallet, item in grouped.items():
        item["wallet"] = wallet
        item["total_pnl_bid_usd"] = item["realized_pnl_usd"] + item["unrealized_pnl_bid_usd"]
        item["total_pnl_mid_usd"] = item["realized_pnl_usd"] + item["unrealized_pnl_mid_usd"]
        rows.append(item)

    if not rows:
        return "Лидеры\nданных по лидерам пока нет"

    rows.sort(key=lambda x: x["total_pnl_bid_usd"], reverse=True)
    lines = ["Лидеры по PnL бота"]
    for idx, row in enumerate(rows[:10], start=1):
        lines.extend(
            [
                f"{idx}. {row['name']} | {row['category']}",
                (
                    f"   total bid {_money(row['total_pnl_bid_usd'], signed=True)} | "
                    f"realized {_money(row['realized_pnl_usd'], signed=True)} | "
                    f"open bid/mid {_money(row['unrealized_pnl_bid_usd'], signed=True)} / "
                    f"{_money(row['unrealized_pnl_mid_usd'], signed=True)}"
                ),
                (
                    f"   entries {row['entries']} | exits {row['exits']} | "
                    f"open invested {_money(row['invested_open_usd'])}"
                ),
            ]
        )
        if row["no_bid_invested_usd"] > 0:
            lines.append(f"   no-bid invested {_money(row['no_bid_invested_usd'])}")

    return "\n".join(lines)


def build_activity_report(*, now: datetime | None = None) -> str:
    init_db()
    init_signal_observation_table()
    now = now or datetime.now(timezone.utc)
    since = now - timedelta(hours=24)

    observations = [
        row
        for row in list_signal_observations(limit=100000)
        if (_safe_dt(row.get("observed_at")) or datetime.min.replace(tzinfo=timezone.utc)) >= since
    ]
    history = [
        row
        for row in list_trade_history(limit=100000)
        if (_safe_dt(row.get("event_time")) or datetime.min.replace(tzinfo=timezone.utc)) >= since
    ]

    status_counts = Counter(str(row.get("latest_status") or "UNKNOWN") for row in observations)
    unique_by_status: dict[str, int] = {}
    for status in status_counts:
        unique_by_status[status] = _unique_count(
            [row for row in observations if str(row.get("latest_status") or "UNKNOWN") == status],
            "latest_trade_hash",
        )
    selected_count = sum(1 for row in observations if row.get("selected_signal_id"))
    by_leader: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "observations": 0,
            "selected": 0,
            "entries": 0,
            "exits": 0,
            "name": "UNKNOWN",
            "category": "UNKNOWN",
        }
    )
    for row in observations:
        wallet = str(row.get("leader_wallet") or "")
        item = by_leader[wallet]
        item["observations"] += 1
        if row.get("selected_signal_id"):
            item["selected"] += 1
        item["name"] = _leader_name(row)
        item["category"] = row.get("category") or item["category"]

    entries = sum(1 for row in history if row.get("event_type") == "ENTRY")
    exits = sum(1 for row in history if row.get("event_type") == "EXIT")
    realized = sum(_safe_float(row.get("realized_pnl_usd")) for row in history if row.get("event_type") == "EXIT")
    for row in history:
        wallet = str(row.get("leader_wallet") or "")
        item = by_leader[wallet]
        item["name"] = _leader_name(row)
        item["category"] = row.get("category") or item["category"]
        if row.get("event_type") == "ENTRY":
            item["entries"] += 1
        if row.get("event_type") == "EXIT":
            item["exits"] += 1

    lines = [
        "Активность за 24ч",
        (
            f"проверки: {len(observations)} | "
            f"уникальные latest-сделки: {_unique_count(observations, 'latest_trade_hash')}"
        ),
        f"выбранные сигналы: {selected_count} проверок",
        f"сделки бота: BUY {entries} | SELL {exits} | realized {_money(realized, signed=True)}",
        "",
        "Пояснение: проверки = каждый polling-цикл. Unique = разные сделки лидеров.",
        "Unique по статусам могут пересекаться, если один trade менял статус между проверками.",
        "Selected = лучший пригодный сигнал-кандидат; он не всегда превращается в BUY/SELL.",
    ]

    if status_counts:
        lines.append("")
        lines.append("Статусы:")
        for status, count in status_counts.most_common(6):
            lines.append(f"{status}: {count} проверок / {unique_by_status.get(status, 0)} unique")
            lines.append(f"  {_status_hint(status)}")

    leaders = sorted(by_leader.values(), key=lambda x: (x["selected"], x["observations"]), reverse=True)
    if leaders:
        lines.append("")
        lines.append("Топ лидеров:")
        for idx, row in enumerate(leaders[:5], start=1):
            lines.append(
                (
                    f"{idx}. {row['name']} | {row['category']} | "
                    f"checks {row['observations']} | selected {row['selected']} | "
                    f"BUY {row['entries']} | SELL {row['exits']}"
                )
            )

    return "\n".join(lines)


def build_latency_report(config: dict[str, Any] | None = None) -> str:
    config = config or {}
    market_cfg = config.get("market_cache", {})
    onchain_cfg = config.get("onchain_shadow", {})
    cache_max_age = float(market_cfg.get("max_age_sec", 5.0))
    cache = market_cache_summary(max_age_sec=cache_max_age)
    onchain = onchain_shadow_summary(hours=24)

    lines = [
        "Latency / источники",
        "",
        "Market WebSocket cache",
        f"токены в cache: {cache['total_tokens']} | свежие <= {cache_max_age:.0f}s: {cache['fresh_tokens']}",
        (
            f"service: {'enabled' if market_cfg.get('enabled', False) else 'disabled'} | "
            f"refresh {float(market_cfg.get('refresh_sec', 60.0)):.0f}s"
        ),
        "",
        "On-chain shadow 24h",
        (
            f"fills: {onchain['fills']} | matched Data API: {onchain['matched_data_api']} | "
            f"unmatched: {onchain['unmatched_data_api']}"
        ),
        f"avg Data API lag after on-chain seen: {_age(onchain.get('avg_data_api_lag_sec'))}",
        (
            f"service: {'enabled' if onchain_cfg.get('enabled', False) else 'disabled'} | "
            f"poll {float(onchain_cfg.get('poll_interval_sec', 4.0)):.0f}s"
        ),
    ]

    latest = cache.get("latest") or []
    if latest:
        lines.append("")
        lines.append("Последние cache updates:")
        for row in latest[:5]:
            lines.append(
                (
                    f"{_short(row.get('token_id') or '')} | {row.get('event_type') or 'event'} | "
                    f"bid {_num(row.get('best_bid'))} ask {_num(row.get('best_ask'))} "
                    f"spread {_num(row.get('spread'))}"
                )
            )

    by_leader = onchain.get("by_leader") or []
    if by_leader:
        lines.append("")
        lines.append("On-chain по лидерам:")
        for row in by_leader[:5]:
            lines.append(
                f"{_short(row.get('leader_wallet') or '')} | {row.get('side')} | fills {row.get('fills')}"
            )

    return "\n".join(lines)


def build_sizing_report(config: dict[str, Any] | None = None) -> str:
    init_db()
    init_signal_observation_table()
    config = config or {}

    registry = [
        row
        for row in list_leader_registry(limit=100000)
        if str(row.get("leader_status") or "").upper() == "ACTIVE"
    ]
    if not registry:
        return "Sizing / эффективность капитала\nнет ACTIVE лидеров"

    open_positions = list_open_positions(limit=100000)
    observations = list_signal_observations(limit=100000)
    processed_signals = list_processed_signals(limit=100000)
    trade_history = list_trade_history(limit=100000)

    rows = []
    for row in registry:
        decision = compute_adaptive_sizing_decision(
            leader_wallet=str(row.get("wallet") or ""),
            leader_budget_usd=_safe_float(row.get("target_budget_usd")),
            config=config,
            open_positions=open_positions,
            observations=observations,
            processed_signals=processed_signals,
            trade_history=trade_history,
        )
        details = decision.details
        rows.append(
            {
                "name": _leader_name(row),
                "category": row.get("category") or "UNKNOWN",
                "budget": _safe_float(row.get("target_budget_usd")),
                "multiplier": decision.multiplier,
                "historical": decision.historical_multiplier,
                "utilization_multiplier": decision.utilization_multiplier,
                "reason": decision.reason,
                "open_exposure": _safe_float(details.get("open_exposure_usd")),
                "utilization": _safe_float(details.get("utilization")),
                "selected_buy_signals": int(_safe_float(details.get("selected_buy_signals"))),
                "usable_demand_signals": int(_safe_float(details.get("usable_demand_signals"))),
                "demand": _safe_float(details.get("selected_buy_demand_usd")),
                "target_capacity": _safe_float(details.get("target_capacity_usd")),
                "median_trade_fraction_7d": _safe_float(details.get("median_trade_fraction_7d")),
                "raw_executable_share_7d": _safe_float(details.get("raw_executable_buy_share_7d")),
                "batch_executable_share_7d": _safe_float(details.get("batch_executable_buy_share_7d")),
                "batch_executable_orders_7d": int(
                    _safe_float(details.get("batch_executable_buy_orders_7d"))
                ),
                "dust_share_7d": _safe_float(details.get("dust_buy_share_7d")),
                "idle_share_7d": _safe_float(details.get("idle_capacity_share_7d")),
                "idle_usd_7d": _safe_float(details.get("idle_capacity_usd_7d")),
                "raw_executable_share_30d": _safe_float(details.get("raw_executable_buy_share_30d")),
                "batch_executable_share_30d": _safe_float(details.get("batch_executable_buy_share_30d")),
                "dust_share_30d": _safe_float(details.get("dust_buy_share_30d")),
                "idle_share_30d": _safe_float(details.get("idle_capacity_share_30d")),
                "budget_skips": int(_safe_float(details.get("budget_skips"))),
                "budget_skip_ratio": _safe_float(details.get("budget_skip_ratio")),
                "budget_skip_multiplier": _safe_float(details.get("budget_skip_multiplier")) or 1.0,
                "entries": int(_safe_float(details.get("executed_entries"))),
                "entry_amount": _safe_float(details.get("executed_entry_amount_usd")),
            }
        )

    rows.sort(
        key=lambda row: (
            row["multiplier"],
            -row["budget_skips"],
            -row["utilization"],
        )
    )

    lines = [
        "Sizing / эффективность капитала",
        "Пояснение: multiplier умножает BUY размер. Чем ниже, тем сильнее бот дробит входы.",
        "",
    ]
    for idx, row in enumerate(rows[:8], start=1):
        pressure = row["demand"] / row["target_capacity"] if row["target_capacity"] > 0 else 0.0
        lines.extend(
            [
                f"{idx}. {row['name']} | {row['category']}",
                (
                    f"   budget {_money(row['budget'])} | open {_money(row['open_exposure'])} "
                    f"| util {_pct(row['utilization'])}"
                ),
                (
                    f"   multiplier {row['multiplier']:.2f} "
                    f"(hist {row['historical']:.2f} x util {row['utilization_multiplier']:.2f})"
                ),
                (
                    f"   BUY signals {row['selected_buy_signals']} | demand {_money(row['demand'])} "
                    f"| target {_money(row['target_capacity'])} | pressure {pressure:.1f}x"
                ),
                (
                    f"   7d median fraction {_pct(row['median_trade_fraction_7d'])} | "
                    f"direct {_pct(row['raw_executable_share_7d'])} | "
                    f"batch {_pct(row['batch_executable_share_7d'])} "
                    f"({row['batch_executable_orders_7d']} orders) | "
                    f"dust {_pct(row['dust_share_7d'])}"
                ),
                (
                    f"   idle capacity 7d {_money(row['idle_usd_7d'])} "
                    f"({_pct(row['idle_share_7d'])})"
                ),
                (
                    f"   30d direct/batch/dust/idle "
                    f"{_pct(row['raw_executable_share_30d'])}/"
                    f"{_pct(row['batch_executable_share_30d'])}/"
                    f"{_pct(row['dust_share_30d'])}/"
                    f"{_pct(row['idle_share_30d'])}"
                ),
                (
                    f"   entries {row['entries']} / {_money(row['entry_amount'])} "
                    f"| budget skips {row['budget_skips']} ({_pct(row['budget_skip_ratio'])}) | {row['reason']}"
                ),
            ]
        )

    return "\n".join(lines)


def build_blocks_report(*, now: datetime | None = None) -> str:
    init_db()
    init_signal_observation_table()
    now = now or datetime.now(timezone.utc)
    since = now - timedelta(hours=24)

    observations = [
        row
        for row in list_signal_observations(limit=100000)
        if (_safe_dt(row.get("observed_at")) or datetime.min.replace(tzinfo=timezone.utc)) >= since
    ]
    blocked = [
        row
        for row in observations
        if row.get("latest_status") in {"POLICY_BLOCKED", "DRIFT_BLOCKED"}
    ]

    if not blocked:
        return "Блокировки за 24ч\nнет POLICY_BLOCKED или DRIFT_BLOCKED"

    by_status: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_key: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in blocked:
        status = str(row.get("latest_status") or "UNKNOWN")
        by_status[status].append(row)
        by_key[_block_key(row)].append(row)

    unique_blocks = [_summarize_block(rows) for rows in by_key.values()]
    unique_blocks.sort(
        key=lambda row: (
            row["observed_at"] or datetime.min.replace(tzinfo=timezone.utc),
            row["checks"],
        ),
        reverse=True,
    )

    lines = [
        "Блокировки за 24ч",
        f"проверки: {len(blocked)} | unique latest-сделки: {len(unique_blocks)}",
        "Пояснение: checks = повторные polling-проверки одной и той же latest-сделки.",
        "",
    ]
    for status in ("POLICY_BLOCKED", "DRIFT_BLOCKED"):
        rows = by_status.get(status, [])
        if not rows:
            continue
        unique = len({_block_key(row) for row in rows})
        lines.append(f"{status}: {len(rows)} проверок / {unique} unique")
        lines.append(f"  {_status_hint(status)}")

    reason_counts: Counter[str] = Counter()
    reason_check_counts: Counter[str] = Counter()
    for item in unique_blocks:
        bucket = str(item["reason_bucket"])
        reason_counts[bucket] += 1
        reason_check_counts[bucket] += int(item["checks"])
    if reason_counts:
        lines.append("")
        lines.append("Причины, unique:")
        for reason, unique in reason_counts.most_common(6):
            lines.append(f"{reason}: {unique} unique / {reason_check_counts[reason]} checks")

    lines.append("")
    lines.append("Последние unique-блокировки:")
    for idx, item in enumerate(unique_blocks[:8], start=1):
        spread_rel = item.get("spread_rel")
        spread_rel_text = f" ({_pct(spread_rel)})" if spread_rel is not None else ""
        age_first = _age(item.get("age_first"))
        age_last = _age(item.get("age_last"))
        age_text = age_last if age_first == age_last else f"{age_first}->{age_last}"
        lines.extend(
            [
                (
                    f"{idx}. {item['leader']} | {item['category']} | {item['side']} | "
                    f"{item['status']} / {item['reason_bucket']} | checks {item['checks']}"
                ),
                (
                    f"   age {age_text} | leader px {_num(item.get('leader_price'))} | "
                    f"mid {_num(item.get('midpoint'))} | spread {_num(item.get('spread'))}{spread_rel_text}"
                ),
                f"   reason: {item['reason'][:120]}",
            ]
        )
        if item.get("hash") or item.get("token"):
            lines.append(
                f"   hash {_short(item.get('hash') or 'n/a')} | token {_short(item.get('token') or 'n/a')}"
            )

    drift_abs_values = [
        value
        for value in (_drift_abs(row.get("latest_reason")) for row in by_status.get("DRIFT_BLOCKED", []))
        if value is not None
    ]
    if drift_abs_values:
        drift_abs_values.sort()
        median = drift_abs_values[len(drift_abs_values) // 2]
        lines.append(f"drift abs: median {median:.4f}, max {max(drift_abs_values):.4f}")

    return "\n".join(lines)


def build_help_report() -> str:
    return "\n".join(
        [
            "Команды Polymarket bot",
            "/status - баланс, equity, алерты, свежесть данных",
            "/positions - открытые позиции и mark-to-market",
            "/leaders - PnL по лидерам из истории бота",
            "/activity - активность сигналов за 24ч",
            "/blocks - policy/drift блокировки за 24ч",
            "/unmarked - позиции без текущего рыночного mark-to-market",
            "/settlements - resolved позиции и последние redeem-операции",
            "/latency - WebSocket cache и on-chain/Data API lag",
            "/sizing - adaptive sizing, utilization и budget pressure по лидерам",
            "/rebalance - прислать review-файлы и запросить подтверждение",
            "/unwind - ручной рыночный выход по лидеру или всем позициям",
            "candidates CATEGORY - топ кандидатов категории pending-review",
            "pick CATEGORY N - выбрать кандидата вручную",
            "/help - это меню",
        ]
    )
