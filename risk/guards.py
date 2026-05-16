from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from execution.state_store import (
    get_leader_registry,
    list_open_positions,
    sum_realized_pnl_since,
)
from risk.limits import RiskLimits


EPS = 1e-9


@dataclass(frozen=True)
class RiskDecision:
    allowed: bool
    reason: str
    details: dict[str, Any] = field(default_factory=dict)


def _safe_float(value: Any) -> float:
    try:
        return float(value) if value is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _position_category(row: dict[str, Any], wallet_categories: dict[str, str]) -> str | None:
    category = row.get("category")
    if category:
        return str(category)

    wallet = row.get("leader_wallet")
    if wallet is None:
        return None
    return wallet_categories.get(str(wallet))


def _exposure_after(before: float, amount_usd: float) -> float:
    return round(before + amount_usd, 8)


def _positive_float_or_none(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _risk_uses_percent_limits(config: dict[str, Any]) -> bool:
    risk = config.get("risk", {})
    return any(
        _positive_float_or_none(risk.get(key)) is not None
        for key in (
            "max_per_trade_pct",
            "max_position_pct",
            "max_wallet_exposure_pct",
            "max_category_exposure_pct",
            "max_portfolio_exposure_pct",
            "max_daily_realized_loss_pct",
        )
    )


def build_runtime_risk_limits(config: dict[str, Any]) -> RiskLimits:
    capital_base_usd = _positive_float_or_none(
        config.get("capital", {}).get("total_capital_usd")
    )
    capital_base_error = None

    if _risk_uses_percent_limits(config):
        capital_cfg = config.get("capital", {})
        source = str(capital_cfg.get("source", "")).strip().lower()
        if source in {"collateral_balance", "account_balance", "wallet_balance"}:
            try:
                from execution.allowance import fetch_collateral_balance_allowance

                snapshot = fetch_collateral_balance_allowance(config)
                capital_base_usd = snapshot.balance_usd
            except Exception as e:
                capital_base_usd = None
                capital_base_error = str(e)

    return RiskLimits.from_config(
        config,
        capital_base_usd=capital_base_usd,
        capital_base_error=capital_base_error,
    )


def evaluate_buy_guards(
    *,
    limits: RiskLimits,
    leader_wallet: str,
    token_id: str,
    amount_usd: float,
    leader_budget_usd: float,
    category: str | None,
    open_positions: list[dict[str, Any]],
    wallet_categories: dict[str, str] | None = None,
    realized_pnl_today_usd: float = 0.0,
) -> RiskDecision:
    wallet_categories = wallet_categories or {}
    amount_usd = _safe_float(amount_usd)
    leader_budget_usd = _safe_float(leader_budget_usd)

    token_exposure = 0.0
    wallet_exposure = 0.0
    category_exposure = 0.0
    portfolio_exposure = 0.0

    for row in open_positions:
        position_usd = _safe_float(row.get("position_usd"))
        if position_usd <= 0:
            continue

        row_wallet = str(row.get("leader_wallet") or "")
        row_token = str(row.get("token_id") or "")
        row_category = _position_category(row, wallet_categories)

        portfolio_exposure += position_usd
        if row_wallet == leader_wallet:
            wallet_exposure += position_usd
            if row_token == token_id:
                token_exposure += position_usd
        if category and row_category == category:
            category_exposure += position_usd

    details = {
        "amount_usd": round(amount_usd, 8),
        "leader_budget_usd": round(leader_budget_usd, 8),
        "capital_base_usd": limits.capital_base_usd,
        "token_exposure_before_usd": round(token_exposure, 8),
        "token_exposure_after_usd": _exposure_after(token_exposure, amount_usd),
        "wallet_exposure_before_usd": round(wallet_exposure, 8),
        "wallet_exposure_after_usd": _exposure_after(wallet_exposure, amount_usd),
        "category_exposure_before_usd": round(category_exposure, 8),
        "category_exposure_after_usd": _exposure_after(category_exposure, amount_usd),
        "portfolio_exposure_before_usd": round(portfolio_exposure, 8),
        "portfolio_exposure_after_usd": _exposure_after(portfolio_exposure, amount_usd),
        "realized_pnl_today_usd": round(realized_pnl_today_usd, 8),
    }

    if limits.trading_disabled:
        return RiskDecision(False, "trading disabled by risk.trading_disabled", details)

    if limits.capital_base_missing:
        reason = "risk percent limits require account collateral balance"
        if limits.capital_base_error:
            reason = f"{reason}: {limits.capital_base_error}"
        return RiskDecision(False, reason, details)

    if amount_usd <= 0:
        return RiskDecision(False, "amount_usd must be positive", details)

    if amount_usd + EPS < limits.min_order_size_usd:
        return RiskDecision(
            False,
            f"amount {amount_usd:.2f} below min_order_size_usd {limits.min_order_size_usd:.2f}",
            details,
        )

    if amount_usd > limits.max_per_trade_usd + EPS:
        return RiskDecision(
            False,
            f"amount {amount_usd:.2f} above max_per_trade_usd {limits.max_per_trade_usd:.2f}",
            details,
        )

    if limits.max_daily_realized_loss_usd is not None:
        daily_loss = -realized_pnl_today_usd
        if daily_loss >= limits.max_daily_realized_loss_usd - EPS:
            return RiskDecision(
                False,
                (
                    f"daily realized loss {daily_loss:.2f} reached "
                    f"max_daily_realized_loss_usd {limits.max_daily_realized_loss_usd:.2f}"
                ),
                details,
            )

    token_after = token_exposure + amount_usd
    wallet_after = wallet_exposure + amount_usd
    category_after = category_exposure + amount_usd
    portfolio_after = portfolio_exposure + amount_usd

    if (
        limits.enforce_leader_budget_cap
        and leader_budget_usd > 0
        and wallet_after > leader_budget_usd + EPS
    ):
        return RiskDecision(
            False,
            f"wallet exposure {wallet_after:.2f} above leader budget {leader_budget_usd:.2f}",
            details,
        )

    if limits.max_position_usd is not None and token_after > limits.max_position_usd + EPS:
        return RiskDecision(
            False,
            f"token position {token_after:.2f} above max_position_usd {limits.max_position_usd:.2f}",
            details,
        )

    if limits.max_wallet_exposure_usd is not None and wallet_after > limits.max_wallet_exposure_usd + EPS:
        return RiskDecision(
            False,
            (
                f"wallet exposure {wallet_after:.2f} above "
                f"max_wallet_exposure_usd {limits.max_wallet_exposure_usd:.2f}"
            ),
            details,
        )

    if (
        category
        and limits.max_category_exposure_usd is not None
        and category_after > limits.max_category_exposure_usd + EPS
    ):
        return RiskDecision(
            False,
            (
                f"category exposure {category_after:.2f} above "
                f"max_category_exposure_usd {limits.max_category_exposure_usd:.2f}"
            ),
            details,
        )

    if (
        limits.max_portfolio_exposure_usd is not None
        and portfolio_after > limits.max_portfolio_exposure_usd + EPS
    ):
        return RiskDecision(
            False,
            (
                f"portfolio exposure {portfolio_after:.2f} above "
                f"max_portfolio_exposure_usd {limits.max_portfolio_exposure_usd:.2f}"
            ),
            details,
        )

    return RiskDecision(True, "ok", details)


def evaluate_entry_risk(
    *,
    config: dict[str, Any],
    leader_wallet: str,
    token_id: str,
    amount_usd: float,
    leader_budget_usd: float,
    category: str | None,
    now: datetime | None = None,
    limits: RiskLimits | None = None,
) -> RiskDecision:
    limits = limits or build_runtime_risk_limits(config)
    open_positions = list_open_positions(limit=100000)

    wallet_categories: dict[str, str] = {}
    if category:
        wallet_categories[leader_wallet] = category

    for row in open_positions:
        wallet = str(row.get("leader_wallet") or "")
        if not wallet or wallet in wallet_categories:
            continue
        registry = get_leader_registry(wallet)
        if registry and registry.get("category"):
            wallet_categories[wallet] = str(registry["category"])

    now = now or datetime.now(timezone.utc)
    day_start = now.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    realized_pnl_today_usd = sum_realized_pnl_since(day_start)

    return evaluate_buy_guards(
        limits=limits,
        leader_wallet=leader_wallet,
        token_id=token_id,
        amount_usd=amount_usd,
        leader_budget_usd=leader_budget_usd,
        category=category,
        open_positions=open_positions,
        wallet_categories=wallet_categories,
        realized_pnl_today_usd=realized_pnl_today_usd,
    )
