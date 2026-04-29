from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


BUDGET_SKIP_STATUSES = {"SKIPPED_SIZING", "SKIPPED_RISK"}


@dataclass(frozen=True)
class AdaptiveSizingDecision:
    enabled: bool
    multiplier: float
    historical_multiplier: float
    utilization_multiplier: float
    reason: str
    details: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value not in (None, "") else default
    except (TypeError, ValueError):
        return default


def _safe_bool(value: Any, default: bool = False) -> bool:
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


def _leader_open_exposure(leader_wallet: str, open_positions: list[dict[str, Any]]) -> float:
    return round(
        sum(
            _safe_float(row.get("position_usd"))
            for row in open_positions
            if str(row.get("leader_wallet") or "") == leader_wallet
        ),
        8,
    )


def _config(config: dict[str, Any]) -> dict[str, Any]:
    return config.get("adaptive_sizing", {})


def adaptive_sizing_enabled(config: dict[str, Any]) -> bool:
    return _safe_bool(_config(config).get("enabled"), True)


def _history_since(config: dict[str, Any], *, now: datetime) -> datetime:
    hours = _safe_float(_config(config).get("lookback_hours"), 24.0)
    return now - timedelta(hours=max(hours, 0.1))


def _unique_selected_buy_observations(
    *,
    leader_wallet: str,
    observations: list[dict[str, Any]],
    since: datetime,
) -> list[dict[str, Any]]:
    by_signal: dict[str, dict[str, Any]] = {}
    for row in observations:
        if str(row.get("leader_wallet") or "") != leader_wallet:
            continue
        if str(row.get("selected_side") or "").upper() != "BUY":
            continue
        signal_id = str(row.get("selected_signal_id") or "").strip()
        if not signal_id:
            continue
        observed_at = _safe_dt(row.get("observed_at"))
        if observed_at is None or observed_at < since:
            continue
        previous = by_signal.get(signal_id)
        if previous is None:
            by_signal[signal_id] = row
            continue
        previous_dt = _safe_dt(previous.get("observed_at"))
        if previous_dt is None or observed_at > previous_dt:
            by_signal[signal_id] = row
    return list(by_signal.values())


def _selected_buy_demand_usd(
    *,
    selected_rows: list[dict[str, Any]],
    fallback_budget_usd: float,
    max_leader_trade_budget_fraction: float,
) -> tuple[float, int]:
    demand = 0.0
    usable = 0
    for row in selected_rows:
        budget = _safe_float(row.get("target_budget_usd"), fallback_budget_usd)
        if budget <= 0:
            budget = fallback_budget_usd
        notional = _safe_float(row.get("selected_trade_notional_usd"))
        portfolio_value = _safe_float(row.get("selected_leader_portfolio_value_usd"))
        if budget <= 0 or notional <= 0 or portfolio_value <= 0:
            continue
        fraction = notional / portfolio_value
        if max_leader_trade_budget_fraction > 0:
            fraction = min(fraction, max_leader_trade_budget_fraction)
        demand += budget * fraction
        usable += 1
    return round(demand, 8), usable


def _processed_budget_skips(
    *,
    leader_wallet: str,
    processed_signals: list[dict[str, Any]],
    since: datetime,
) -> int:
    count = 0
    for row in processed_signals:
        if str(row.get("leader_wallet") or "") != leader_wallet:
            continue
        if str(row.get("side") or "").upper() != "BUY":
            continue
        created_at = _safe_dt(row.get("created_at"))
        if created_at is None or created_at < since:
            continue
        status = str(row.get("status") or "")
        reason = str(row.get("reason") or "").lower()
        if status not in BUDGET_SKIP_STATUSES:
            continue
        if "budget" in reason or "exposure" in reason:
            count += 1
    return count


def _executed_entries(
    *,
    leader_wallet: str,
    trade_history: list[dict[str, Any]],
    since: datetime,
) -> tuple[int, float]:
    count = 0
    amount = 0.0
    for row in trade_history:
        if str(row.get("leader_wallet") or "") != leader_wallet:
            continue
        if str(row.get("event_type") or "").upper() != "ENTRY":
            continue
        event_time = _safe_dt(row.get("event_time"))
        if event_time is None or event_time < since:
            continue
        count += 1
        amount += _safe_float(row.get("amount_usd"))
    return count, round(amount, 8)


def _utilization_multiplier(utilization: float, cfg: dict[str, Any]) -> float:
    start = _safe_float(cfg.get("utilization_throttle_start"), 0.60)
    full = _safe_float(cfg.get("utilization_throttle_full"), 0.90)
    floor = _safe_float(cfg.get("min_utilization_multiplier"), 0.25)
    if full <= start:
        return max(min(floor, 1.0), 0.0)
    if utilization <= start:
        return 1.0
    if utilization >= full:
        return floor
    progress = (utilization - start) / (full - start)
    return round(1.0 - progress * (1.0 - floor), 8)


def compute_adaptive_sizing_decision(
    *,
    leader_wallet: str,
    leader_budget_usd: float,
    config: dict[str, Any],
    open_positions: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    processed_signals: list[dict[str, Any]],
    trade_history: list[dict[str, Any]],
    now: datetime | None = None,
) -> AdaptiveSizingDecision:
    cfg = _config(config)
    enabled = adaptive_sizing_enabled(config)
    if not enabled:
        return AdaptiveSizingDecision(
            enabled=False,
            multiplier=1.0,
            historical_multiplier=1.0,
            utilization_multiplier=1.0,
            reason="adaptive sizing disabled",
            details={},
        )

    now = now or datetime.now(timezone.utc)
    since = _history_since(config, now=now)
    budget = _safe_float(leader_budget_usd)
    if budget <= 0:
        return AdaptiveSizingDecision(
            enabled=True,
            multiplier=1.0,
            historical_multiplier=1.0,
            utilization_multiplier=1.0,
            reason="leader budget <= 0",
            details={"leader_budget_usd": budget},
        )

    open_exposure = _leader_open_exposure(leader_wallet, open_positions)
    utilization = open_exposure / budget if budget > 0 else 0.0
    selected_buys = _unique_selected_buy_observations(
        leader_wallet=leader_wallet,
        observations=observations,
        since=since,
    )
    max_budget_fraction = _safe_float(
        config.get("sizing", {}).get("max_leader_trade_budget_fraction"),
        0.0,
    )
    demand_usd, usable_demand_signals = _selected_buy_demand_usd(
        selected_rows=selected_buys,
        fallback_budget_usd=budget,
        max_leader_trade_budget_fraction=max_budget_fraction,
    )

    min_signals = int(_safe_float(cfg.get("min_buy_signals_for_history"), 5))
    target_turnover = _safe_float(cfg.get("target_budget_turnover"), 0.85)
    min_historical = _safe_float(cfg.get("min_historical_multiplier"), 0.20)
    max_historical = _safe_float(cfg.get("max_historical_multiplier"), 1.0)
    target_capacity = max(budget * target_turnover, 0.0)

    if usable_demand_signals >= min_signals and demand_usd > target_capacity > 0:
        historical_multiplier = target_capacity / demand_usd
        historical_multiplier = max(min_historical, min(max_historical, historical_multiplier))
        historical_reason = "historical demand pressure"
    else:
        historical_multiplier = 1.0
        historical_reason = "insufficient pressure history"

    utilization_mult = _utilization_multiplier(utilization, cfg)
    raw_multiplier = historical_multiplier * utilization_mult
    min_multiplier = _safe_float(cfg.get("min_multiplier"), 0.10)
    max_multiplier = _safe_float(cfg.get("max_multiplier"), 1.0)
    multiplier = max(min_multiplier, min(max_multiplier, raw_multiplier))

    skipped_by_budget = _processed_budget_skips(
        leader_wallet=leader_wallet,
        processed_signals=processed_signals,
        since=since,
    )
    entry_count, entry_amount = _executed_entries(
        leader_wallet=leader_wallet,
        trade_history=trade_history,
        since=since,
    )

    reasons = []
    if historical_multiplier < 1.0:
        reasons.append(historical_reason)
    if utilization_mult < 1.0:
        reasons.append("live utilization throttle")
    if not reasons:
        reasons.append("full size")

    details = {
        "leader_budget_usd": round(budget, 8),
        "open_exposure_usd": open_exposure,
        "utilization": round(utilization, 8),
        "lookback_hours": _safe_float(cfg.get("lookback_hours"), 24.0),
        "selected_buy_signals": len(selected_buys),
        "usable_demand_signals": usable_demand_signals,
        "selected_buy_demand_usd": demand_usd,
        "target_capacity_usd": round(target_capacity, 8),
        "budget_skips": skipped_by_budget,
        "executed_entries": entry_count,
        "executed_entry_amount_usd": entry_amount,
        "historical_multiplier_reason": historical_reason,
    }

    return AdaptiveSizingDecision(
        enabled=True,
        multiplier=round(multiplier, 8),
        historical_multiplier=round(historical_multiplier, 8),
        utilization_multiplier=round(utilization_mult, 8),
        reason=", ".join(reasons),
        details=details,
    )


def neutral_adaptive_sizing_decision(reason: str, *, enabled: bool = True) -> AdaptiveSizingDecision:
    return AdaptiveSizingDecision(
        enabled=enabled,
        multiplier=1.0,
        historical_multiplier=1.0,
        utilization_multiplier=1.0,
        reason=reason,
        details={},
    )
