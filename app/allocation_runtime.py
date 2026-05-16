from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any, Callable


REBALANCE_CONFIG = Path("config/rebalance.toml")


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value not in (None, "") else default
    except (TypeError, ValueError):
        return default


def load_rebalance_config(path: Path = REBALANCE_CONFIG) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("rb") as f:
        return tomllib.load(f)


def resolve_total_capital_usd(
    *,
    executor_config: dict[str, Any] | None = None,
    rebalance_config: dict[str, Any] | None = None,
    balance_loader: Callable[[dict[str, Any]], float] | None = None,
    default: float = 0.0,
    allow_zero_collateral_balance: bool = False,
) -> float:
    executor_config = executor_config or {}
    rebalance_config = rebalance_config if rebalance_config is not None else load_rebalance_config()
    capital_cfg = executor_config.get("capital", {})
    source = str(capital_cfg.get("source", "")).strip().lower()

    if source in {"collateral_balance", "account_balance", "wallet_balance"}:
        if balance_loader is None:
            from execution.allowance import fetch_collateral_balance_allowance

            balance_loader = lambda cfg: fetch_collateral_balance_allowance(cfg).balance_usd
        balance = _safe_float(balance_loader(executor_config), 0.0)
        if balance > 0:
            return round(balance, 2)
        if allow_zero_collateral_balance:
            return 0.0
        raise RuntimeError("capital.source requires a positive collateral balance")

    executor_capital = _safe_float(
        capital_cfg.get("total_capital_usd"),
        0.0,
    )
    if executor_capital > 0:
        return executor_capital

    rebalance_capital = _safe_float(
        rebalance_config.get("capital", {}).get("total_capital_usd"),
        0.0,
    )
    if rebalance_capital > 0:
        return rebalance_capital

    return default


def resolve_leader_budget_usd(row: dict[str, Any], *, total_capital_usd: float) -> float:
    explicit_budget = _safe_float(row.get("target_budget_usd"), 0.0)
    if explicit_budget > 0:
        return round(explicit_budget, 2)

    weight = _safe_float(row.get("target_weight"), 0.0)
    if weight <= 0:
        weight = _safe_float(row.get("weight"), 0.0)

    return round(total_capital_usd * weight, 2)
