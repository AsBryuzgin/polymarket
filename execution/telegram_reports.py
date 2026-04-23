from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from execution.allowance import fetch_collateral_balance_allowance
from execution.polymarket_executor import fetch_market_snapshot
from execution.signal_observation_store import (
    init_signal_observation_table,
    list_signal_observations,
)
from execution.state_store import (
    init_db,
    list_leader_registry,
    list_open_positions,
    list_trade_history,
)


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
    spread_rel = None
    if midpoint_num and spread_num is not None:
        spread_rel = spread_num / midpoint_num
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
        position_usd = _safe_float(pos.get("position_usd"))
        avg_entry_price = _safe_float(pos.get("avg_entry_price"))
        qty = position_usd / avg_entry_price if avg_entry_price > 0 else 0.0
        mark_bid = 0.0
        mark_mid = 0.0
        snapshot_status = "OK"

        try:
            snapshot = snapshot_loader(str(pos["token_id"]), "SELL")
            best_bid = _safe_float(snapshot.get("best_bid"))
            midpoint = _safe_float(snapshot.get("midpoint"))
            mark_bid = qty * best_bid if best_bid > 0 else 0.0
            mark_mid = qty * midpoint if midpoint > 0 else 0.0
        except Exception as e:
            snapshot_status = "ERROR"
            errors.append(f"{_short(str(pos.get('token_id')))}: {e}")

        row = dict(pos)
        row.update(
            {
                "qty": qty,
                "mark_value_bid_usd": mark_bid,
                "mark_value_mid_usd": mark_mid,
                "unrealized_pnl_bid_usd": mark_bid - position_usd,
                "unrealized_pnl_mid_usd": mark_mid - position_usd,
                "snapshot_status": snapshot_status,
            }
        )
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
    observations = list_signal_observations(limit=1)

    invested = sum(_safe_float(row.get("position_usd")) for row in open_rows)
    mark_bid = sum(_safe_float(row.get("mark_value_bid_usd")) for row in open_rows)
    mark_mid = sum(_safe_float(row.get("mark_value_mid_usd")) for row in open_rows)

    mode = str(config.get("global", {}).get("execution_mode", "unknown")).lower()
    paper_bankroll = _configured_capital_usd(config) if mode == "paper" else None
    funding = None
    funding_error = None

    if paper_bankroll is not None:
        cash = max(paper_bankroll - invested, 0.0)
        allowance = None
    else:
        funding, funding_error = _funding_snapshot(config)
        cash = funding.get("balance_usd") if funding else None
        allowance = funding.get("allowance_usd") if funding else None

    total_bid = cash + mark_bid if cash is not None else None
    total_mid = cash + mark_mid if cash is not None else None

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
                f"{_money(mark_bid - invested, signed=True)} / "
                f"{_money(mark_mid - invested, signed=True)}"
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
    if funding_error:
        lines.append(f"проверка баланса: ERROR {_short(funding_error, 40, 0)}")
    if snapshot_errors:
        lines.append(f"ошибки market snapshot: {len(snapshot_errors)}")

    return "\n".join(lines)


def build_positions_report(
    *,
    snapshot_loader: SnapshotLoader = fetch_market_snapshot,
) -> str:
    init_db()
    open_rows, snapshot_errors = _open_position_marks(snapshot_loader=snapshot_loader)

    if not open_rows:
        return "Открытые позиции\nпозиций нет"

    invested = sum(_safe_float(row.get("position_usd")) for row in open_rows)
    mark_bid = sum(_safe_float(row.get("mark_value_bid_usd")) for row in open_rows)
    lines = [
        "Открытые позиции",
        f"Всего: {len(open_rows)} | вложено: {_money(invested)} | bid PnL: {_money(mark_bid - invested, signed=True)}",
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
                    f"{_money(row.get('unrealized_pnl_bid_usd'), signed=True)}"
                ),
                f"   token {_short(str(row.get('token_id')))}",
            ]
        )

    if len(open_rows) > 12:
        lines.append(f"... еще {len(open_rows) - 12}")
    if snapshot_errors:
        lines.append(f"ошибки market snapshot: {len(snapshot_errors)}")

    return "\n".join(lines)


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
            "invested_open_usd": 0.0,
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
        item["unrealized_pnl_bid_usd"] += _safe_float(row.get("unrealized_pnl_bid_usd"))
        item["invested_open_usd"] += _safe_float(row.get("position_usd"))

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
                    f"   total {_money(row['total_pnl_bid_usd'], signed=True)} | "
                    f"realized {_money(row['realized_pnl_usd'], signed=True)} | "
                    f"open {_money(row['unrealized_pnl_bid_usd'], signed=True)}"
                ),
                (
                    f"   entries {row['entries']} | exits {row['exits']} | "
                    f"open invested {_money(row['invested_open_usd'])}"
                ),
            ]
        )

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
        "Selected = лучший пригодный сигнал-кандидат; он не всегда превращается в BUY/SELL.",
    ]

    if status_counts:
        lines.append("")
        lines.append("Статусы, unique:")
        for status, count in status_counts.most_common(6):
            lines.append(f"{status}: {unique_by_status.get(status, 0)}")
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
            "/rebalance - прислать review-файлы и запросить подтверждение",
            "candidates CATEGORY - топ кандидатов категории pending-review",
            "pick CATEGORY N - выбрать кандидата вручную",
            "/help - это меню",
        ]
    )
