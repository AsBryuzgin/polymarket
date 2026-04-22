from __future__ import annotations

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


def build_status_report(
    config: dict[str, Any],
    *,
    now: datetime | None = None,
    snapshot_loader: SnapshotLoader = fetch_market_snapshot,
) -> str:
    init_db()
    init_signal_observation_table()
    now = now or datetime.now(timezone.utc)

    funding, funding_error = _funding_snapshot(config)
    open_rows, snapshot_errors = _open_position_marks(snapshot_loader=snapshot_loader)
    registry = list_leader_registry(limit=100000)
    observations = list_signal_observations(limit=1)

    cash = funding.get("balance_usd") if funding else None
    allowance = funding.get("allowance_usd") if funding else None
    invested = sum(_safe_float(row.get("position_usd")) for row in open_rows)
    mark_bid = sum(_safe_float(row.get("mark_value_bid_usd")) for row in open_rows)
    mark_mid = sum(_safe_float(row.get("mark_value_mid_usd")) for row in open_rows)
    total_bid = cash + mark_bid if cash is not None else None
    total_mid = cash + mark_mid if cash is not None else None

    active_leaders = sum(1 for row in registry if row.get("leader_status") == "ACTIVE")
    exit_only_leaders = sum(1 for row in registry if row.get("leader_status") == "EXIT_ONLY")
    last_observed_at = _safe_dt(observations[0].get("observed_at")) if observations else None
    last_age = _age_minutes(last_observed_at, now=now)
    alert_count = _load_latest_alert_count()

    lines = [
        "Polymarket bot status",
        f"mode: {str(config.get('global', {}).get('execution_mode', 'unknown')).upper()}",
        f"cash balance: {_money(cash)}",
        f"allowance: {_money(allowance)}",
        f"open positions: {len(open_rows)} | invested: {_money(invested)}",
        f"equity by bid: {_money(total_bid)} | by mid: {_money(total_mid)}",
        f"open PnL by bid: {_money(mark_bid - invested, signed=True)} | by mid: {_money(mark_mid - invested, signed=True)}",
        f"leaders: {active_leaders} active, {exit_only_leaders} exit-only",
    ]

    if last_age is not None:
        lines.append(f"last observation: {last_age:.1f} min ago")
    if alert_count is not None:
        lines.append(f"current alerts: {alert_count}")
    if funding_error:
        lines.append(f"funding check: ERROR {_short(funding_error, 40, 0)}")
    if snapshot_errors:
        lines.append(f"snapshot errors: {len(snapshot_errors)}")

    return "\n".join(lines)


def build_positions_report(
    *,
    snapshot_loader: SnapshotLoader = fetch_market_snapshot,
) -> str:
    init_db()
    open_rows, snapshot_errors = _open_position_marks(snapshot_loader=snapshot_loader)

    if not open_rows:
        return "Open positions\nnone"

    lines = ["Open positions"]
    registry = {row["wallet"]: row for row in list_leader_registry(limit=100000)}
    for row in sorted(open_rows, key=lambda x: _safe_float(x.get("position_usd")), reverse=True)[:12]:
        leader = registry.get(row.get("leader_wallet"), {})
        name = _leader_name({**leader, **row})
        category = leader.get("category") or "UNKNOWN"
        lines.append(
            (
                f"{name} | {category} | {_money(row.get('position_usd'))} "
                f"-> bid {_money(row.get('mark_value_bid_usd'))} "
                f"pnl {_money(row.get('unrealized_pnl_bid_usd'), signed=True)} "
                f"| token {_short(str(row.get('token_id')))}"
            )
        )

    if len(open_rows) > 12:
        lines.append(f"... {len(open_rows) - 12} more")
    if snapshot_errors:
        lines.append(f"snapshot errors: {len(snapshot_errors)}")

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

    active_wallets = {str(row.get("wallet")) for row in registry if row.get("leader_status") == "ACTIVE"}
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
        return "Leaders\nno leader data yet"

    rows.sort(key=lambda x: x["total_pnl_bid_usd"], reverse=True)
    lines = ["Leaders by bot PnL"]
    for row in rows[:10]:
        lines.append(
            (
                f"{row['name']} | {row['category']} | "
                f"PnL {_money(row['total_pnl_bid_usd'], signed=True)} "
                f"(realized {_money(row['realized_pnl_usd'], signed=True)}, "
                f"open {_money(row['unrealized_pnl_bid_usd'], signed=True)}) | "
                f"entries {row['entries']} exits {row['exits']}"
            )
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
    selected_count = sum(1 for row in observations if row.get("selected_signal_id"))
    by_leader: dict[str, dict[str, Any]] = defaultdict(lambda: {"observations": 0, "selected": 0, "name": "UNKNOWN", "category": "UNKNOWN"})
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

    lines = [
        "Activity 24h",
        f"observations: {len(observations)} | selected: {selected_count}",
        f"entries: {entries} | exits: {exits} | realized: {_money(realized, signed=True)}",
    ]

    if status_counts:
        status_text = ", ".join(f"{status}={count}" for status, count in status_counts.most_common(5))
        lines.append(f"statuses: {status_text}")

    leaders = sorted(by_leader.values(), key=lambda x: (x["selected"], x["observations"]), reverse=True)
    if leaders:
        lines.append("top activity:")
        for row in leaders[:5]:
            lines.append(
                f"{row['name']} | {row['category']} | obs {row['observations']} selected {row['selected']}"
            )

    return "\n".join(lines)


def build_help_report() -> str:
    return "\n".join(
        [
            "Polymarket bot commands",
            "/status - balance, equity, alerts, freshness",
            "/positions - open positions and mark-to-market",
            "/leaders - leader PnL from bot history",
            "/activity - signal activity over the last 24h",
            "/help - this menu",
        ]
    )
