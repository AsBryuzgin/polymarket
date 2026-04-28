from __future__ import annotations

from typing import Any

V1_EXCHANGE_ADDRESSES = {
    "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e",
    "0xc5d563a36ae78145c45a50134d48a1215220f80a",
}


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


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _check_min(
    *,
    blockers: list[str],
    checks: dict[str, Any],
    section: dict[str, Any],
    key: str,
    min_value: float,
    label: str,
) -> None:
    value = _float_or_none(section.get(key))
    checks[label] = value
    if value is None:
        blockers.append(f"{label} is missing; expected at least {min_value:g}")
    elif value + 1e-12 < min_value:
        blockers.append(f"{label} {value:g} below required {min_value:g}")


def _check_max(
    *,
    blockers: list[str],
    checks: dict[str, Any],
    section: dict[str, Any],
    key: str,
    max_value: float,
    label: str,
) -> None:
    value = _float_or_none(section.get(key))
    checks[label] = value
    if value is None:
        blockers.append(f"{label} is missing; expected at most {max_value:g}")
    elif value > max_value + 1e-12:
        blockers.append(f"{label} {value:g} above allowed {max_value:g}")


def _check_bool(
    *,
    blockers: list[str],
    checks: dict[str, Any],
    section: dict[str, Any],
    key: str,
    expected: bool,
    label: str,
) -> None:
    value = _bool_or_default(section.get(key), not expected)
    checks[label] = value
    if value is not expected:
        blockers.append(f"{label} must be {str(expected).lower()}")


def build_config_safety_report(config: dict[str, Any]) -> dict[str, Any]:
    """Validate runtime knobs that are easy to stale-copy before live cutover."""

    safety_cfg = config.get("config_safety", {})
    enabled = _bool_or_default(safety_cfg.get("enabled"), True)
    if not enabled:
        return {
            "status": "DISABLED",
            "blockers": [],
            "warnings": ["config safety gate is disabled"],
            "checks": {},
        }

    signal_freshness = config.get("signal_freshness", {})
    global_cfg = config.get("global", {})
    alert_delivery = config.get("alert_delivery", {})
    filters = config.get("filters", {})
    risk = config.get("risk", {})
    sizing = config.get("sizing", {})
    live_execution = config.get("live_execution", {})
    runtime_lock = config.get("runtime_lock", {})
    state_backup = config.get("state_backup", {})
    onchain_shadow = config.get("onchain_shadow", {})

    blockers: list[str] = []
    warnings: list[str] = []
    checks: dict[str, Any] = {}

    _check_min(
        blockers=blockers,
        checks=checks,
        section=signal_freshness,
        key="max_recent_trades",
        min_value=float(safety_cfg.get("min_recent_trades", 50)),
        label="signal_freshness.max_recent_trades",
    )
    _check_min(
        blockers=blockers,
        checks=checks,
        section=signal_freshness,
        key="max_signals_per_cycle",
        min_value=float(safety_cfg.get("min_signals_per_cycle", 20)),
        label="signal_freshness.max_signals_per_cycle",
    )
    _check_min(
        blockers=blockers,
        checks=checks,
        section=signal_freshness,
        key="max_price_drift_abs",
        min_value=float(safety_cfg.get("min_price_drift_abs", 0.02)),
        label="signal_freshness.max_price_drift_abs",
    )
    _check_min(
        blockers=blockers,
        checks=checks,
        section=signal_freshness,
        key="max_price_drift_rel",
        min_value=float(safety_cfg.get("min_price_drift_rel", 0.03)),
        label="signal_freshness.max_price_drift_rel",
    )
    _check_min(
        blockers=blockers,
        checks=checks,
        section=filters,
        key="buy_max_price",
        min_value=float(safety_cfg.get("min_buy_max_price", 0.96)),
        label="filters.buy_max_price",
    )
    _check_max(
        blockers=blockers,
        checks=checks,
        section=filters,
        key="buy_max_price",
        max_value=float(safety_cfg.get("max_buy_max_price", 0.99)),
        label="filters.buy_max_price",
    )
    _check_max(
        blockers=blockers,
        checks=checks,
        section=risk,
        key="min_order_size_usd",
        max_value=float(safety_cfg.get("max_min_order_size_usd", 0.01)),
        label="risk.min_order_size_usd",
    )
    _check_max(
        blockers=blockers,
        checks=checks,
        section=risk,
        key="max_per_trade_pct",
        max_value=float(safety_cfg.get("max_per_trade_pct", 0.05)),
        label="risk.max_per_trade_pct",
    )
    _check_max(
        blockers=blockers,
        checks=checks,
        section=risk,
        key="max_position_pct",
        max_value=float(safety_cfg.get("max_position_pct", 0.08)),
        label="risk.max_position_pct",
    )
    _check_max(
        blockers=blockers,
        checks=checks,
        section=risk,
        key="max_portfolio_exposure_pct",
        max_value=float(safety_cfg.get("max_portfolio_exposure_pct", 0.90)),
        label="risk.max_portfolio_exposure_pct",
    )
    _check_max(
        blockers=blockers,
        checks=checks,
        section=risk,
        key="max_daily_realized_loss_pct",
        max_value=float(safety_cfg.get("max_daily_realized_loss_pct", 0.075)),
        label="risk.max_daily_realized_loss_pct",
    )

    _check_bool(
        blockers=blockers,
        checks=checks,
        section=sizing,
        key="round_up_to_min_order",
        expected=True,
        label="sizing.round_up_to_min_order",
    )
    _check_bool(
        blockers=blockers,
        checks=checks,
        section=sizing,
        key="allow_notional_fallback",
        expected=False,
        label="sizing.allow_notional_fallback",
    )
    _check_bool(
        blockers=blockers,
        checks=checks,
        section=sizing,
        key="allow_budget_fallback",
        expected=False,
        label="sizing.allow_budget_fallback",
    )
    _check_bool(
        blockers=blockers,
        checks=checks,
        section=live_execution,
        key="require_verified_fill",
        expected=True,
        label="live_execution.require_verified_fill",
    )
    _check_min(
        blockers=blockers,
        checks=checks,
        section=live_execution,
        key="post_submit_poll_attempts",
        min_value=float(safety_cfg.get("min_post_submit_poll_attempts", 5)),
        label="live_execution.post_submit_poll_attempts",
    )
    _check_bool(
        blockers=blockers,
        checks=checks,
        section=runtime_lock,
        key="enabled",
        expected=True,
        label="runtime_lock.enabled",
    )
    _check_bool(
        blockers=blockers,
        checks=checks,
        section=runtime_lock,
        key="activate_on_critical_alerts",
        expected=True,
        label="runtime_lock.activate_on_critical_alerts",
    )
    _check_bool(
        blockers=blockers,
        checks=checks,
        section=state_backup,
        key="enabled",
        expected=True,
        label="state_backup.enabled",
    )

    exchange_addresses = onchain_shadow.get("exchange_addresses")
    if isinstance(exchange_addresses, list):
        normalized = {
            str(address).strip().lower()
            for address in exchange_addresses
            if str(address).strip()
        }
        checks["onchain_shadow.exchange_addresses"] = sorted(normalized)
        stale = sorted(normalized & V1_EXCHANGE_ADDRESSES)
        if stale:
            blockers.append(
                "onchain_shadow.exchange_addresses still contains deprecated V1 exchange address(es): "
                + ", ".join(stale)
            )

    live_requested = (
        str(global_cfg.get("execution_mode") or "").strip().upper() == "LIVE"
        or _bool_or_default(global_cfg.get("live_trading_enabled"), False)
    )
    require_alert_delivery_for_live = _bool_or_default(
        safety_cfg.get("require_alert_delivery_for_live"),
        True,
    )
    checks["alert_delivery.required_for_live"] = require_alert_delivery_for_live
    checks["alert_delivery.enabled"] = _bool_or_default(alert_delivery.get("enabled"), False)
    if live_requested and require_alert_delivery_for_live:
        _check_bool(
            blockers=blockers,
            checks=checks,
            section=alert_delivery,
            key="enabled",
            expected=True,
            label="alert_delivery.enabled",
        )

    return {
        "status": "GO" if not blockers else "NO_GO",
        "blockers": blockers,
        "warnings": warnings,
        "checks": checks,
    }
