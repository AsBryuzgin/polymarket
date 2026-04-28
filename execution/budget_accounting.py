from __future__ import annotations

from typing import Any, Callable

from execution.state_store import (
    list_leader_registry,
    list_open_positions,
    upsert_leader_registry_row,
)


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


def _wallet(row: dict[str, Any], key: str = "wallet") -> str:
    return str(row.get(key) or "").strip()


def _leader_wallet(row: dict[str, Any]) -> str:
    return str(row.get("leader_wallet") or "").strip()


def _active_weight(row: dict[str, Any]) -> float:
    weight = _safe_float(row.get("target_weight"), 0.0)
    if weight <= 0:
        weight = _safe_float(row.get("weight"), 0.0)
    return max(weight, 0.0)


def _allocate_cents(total_usd: float, rows: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    active_rows = [row for row in rows if _wallet(row)]
    if not active_rows:
        return {}

    weights = [_active_weight(row) for row in active_rows]
    weight_sum = sum(weights)
    if weight_sum <= 0:
        weights = [1.0 for _row in active_rows]
        weight_sum = float(len(active_rows))

    total_cents = max(int(round(max(total_usd, 0.0) * 100)), 0)
    raw_rows: list[dict[str, Any]] = []
    for row, weight in zip(active_rows, weights, strict=True):
        share = weight / weight_sum if weight_sum > 0 else 0.0
        raw_cents = total_cents * share
        floor_cents = int(raw_cents)
        raw_rows.append(
            {
                "wallet": _wallet(row),
                "budget_weight": share,
                "floor_cents": floor_cents,
                "fractional": raw_cents - floor_cents,
            }
        )

    remaining = total_cents - sum(int(row["floor_cents"]) for row in raw_rows)
    for row in sorted(raw_rows, key=lambda item: (-float(item["fractional"]), str(item["wallet"]))):
        if remaining <= 0:
            break
        row["floor_cents"] += 1
        remaining -= 1

    return {
        str(row["wallet"]): {
            "target_budget_usd": round(int(row["floor_cents"]) / 100.0, 2),
            "budget_weight": round(float(row["budget_weight"]), 8),
        }
        for row in raw_rows
    }


def resolve_budget_total_capital_usd(
    *,
    executor_config: dict[str, Any] | None = None,
    rebalance_config: dict[str, Any] | None = None,
    open_positions: list[dict[str, Any]] | None = None,
    balance_loader: Callable[[dict[str, Any]], float] | None = None,
    default: float = 0.0,
    allow_zero_collateral_balance: bool = False,
) -> float:
    executor_config = executor_config or {}
    rebalance_config = rebalance_config or {}
    capital_cfg = executor_config.get("capital", {})
    source = str(capital_cfg.get("source", "")).strip().lower()

    explicit_capital = _safe_float(capital_cfg.get("total_capital_usd"), 0.0)
    if explicit_capital > 0:
        return round(explicit_capital, 2)

    if source in {"collateral_balance", "account_balance", "wallet_balance"}:
        if balance_loader is None:
            from execution.allowance import fetch_collateral_balance_allowance

            balance_loader = lambda cfg: fetch_collateral_balance_allowance(cfg).balance_usd

        balance = _safe_float(balance_loader(executor_config), 0.0)
        include_open = _safe_bool(
            capital_cfg.get("include_open_positions_in_budget_base"),
            True,
        )
        open_cost_basis = 0.0
        if include_open:
            rows = open_positions if open_positions is not None else list_open_positions(limit=100000)
            open_cost_basis = sum(_safe_float(row.get("position_usd"), 0.0) for row in rows)

        total = round(balance + open_cost_basis, 2)
        if total > 0:
            return total
        if allow_zero_collateral_balance:
            return 0.0
        raise RuntimeError("capital.source requires a positive collateral balance or open position base")

    rebalance_capital = _safe_float(
        rebalance_config.get("capital", {}).get("total_capital_usd"),
        0.0,
    )
    if rebalance_capital > 0:
        return round(rebalance_capital, 2)

    return default


def compute_active_budget_plan(
    *,
    total_capital_usd: float,
    registry_rows: list[dict[str, Any]],
    open_positions: list[dict[str, Any]],
) -> dict[str, Any]:
    active_rows = [
        row
        for row in registry_rows
        if str(row.get("leader_status") or "ACTIVE").upper() == "ACTIVE" and _wallet(row)
    ]
    active_wallets = {_wallet(row) for row in active_rows}
    explicit_exit_only_wallets = {
        _wallet(row)
        for row in registry_rows
        if str(row.get("leader_status") or "").upper() == "EXIT_ONLY" and _wallet(row)
    }

    reserve_wallets = set(explicit_exit_only_wallets)
    for row in open_positions:
        wallet = _leader_wallet(row)
        if wallet and wallet not in active_wallets:
            reserve_wallets.add(wallet)

    reserved_positions = [
        row
        for row in open_positions
        if _leader_wallet(row) in reserve_wallets
        and _safe_float(row.get("position_usd"), 0.0) > 0
    ]
    exit_only_reserved_usd = round(
        sum(_safe_float(row.get("position_usd"), 0.0) for row in reserved_positions),
        8,
    )
    active_capital_usd = round(max(_safe_float(total_capital_usd, 0.0) - exit_only_reserved_usd, 0.0), 8)
    allocations_by_wallet = _allocate_cents(active_capital_usd, active_rows)

    allocations = []
    for row in active_rows:
        wallet = _wallet(row)
        allocation = allocations_by_wallet.get(wallet, {"target_budget_usd": 0.0, "budget_weight": 0.0})
        allocations.append(
            {
                "wallet": wallet,
                "user_name": row.get("user_name") or "",
                "category": row.get("category") or "",
                "target_weight": allocation["budget_weight"],
                "target_budget_usd": allocation["target_budget_usd"],
            }
        )

    return {
        "total_capital_usd": round(_safe_float(total_capital_usd, 0.0), 2),
        "exit_only_reserved_usd": round(exit_only_reserved_usd, 2),
        "active_capital_usd": round(active_capital_usd, 2),
        "active_leaders": len(active_rows),
        "exit_only_reserved_positions": len(reserved_positions),
        "exit_only_reserved_wallets": sorted(reserve_wallets),
        "active_budget_total_usd": round(
            sum(float(row["target_budget_usd"]) for row in allocations),
            2,
        ),
        "allocations": allocations,
    }


def refresh_active_budgets_after_exit_reserve(
    *,
    total_capital_usd: float,
    registry_rows: list[dict[str, Any]] | None = None,
    open_positions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    registry_rows = registry_rows if registry_rows is not None else list_leader_registry(limit=100000)
    open_positions = open_positions if open_positions is not None else list_open_positions(limit=100000)
    plan = compute_active_budget_plan(
        total_capital_usd=total_capital_usd,
        registry_rows=registry_rows,
        open_positions=open_positions,
    )
    budget_by_wallet = {row["wallet"]: row for row in plan["allocations"]}

    updated = 0
    for row in registry_rows:
        wallet = _wallet(row)
        if str(row.get("leader_status") or "").upper() != "ACTIVE":
            continue
        allocation = budget_by_wallet.get(wallet)
        if allocation is None:
            continue
        upsert_leader_registry_row(
            wallet=wallet,
            category=str(row.get("category") or ""),
            user_name=str(row.get("user_name") or ""),
            leader_status="ACTIVE",
            target_weight=float(allocation["target_weight"]),
            target_budget_usd=float(allocation["target_budget_usd"]),
            grace_until=row.get("grace_until"),
            source_tag=str(row.get("source_tag") or "budget_refresh"),
        )
        updated += 1

    return {**plan, "updated_active_leaders": updated}


def refresh_active_budgets_from_config(
    *,
    config: dict[str, Any],
    rebalance_config: dict[str, Any] | None = None,
    allow_zero_collateral_balance: bool = True,
) -> dict[str, Any]:
    open_positions = list_open_positions(limit=100000)
    total_capital_usd = resolve_budget_total_capital_usd(
        executor_config=config,
        rebalance_config=rebalance_config,
        open_positions=open_positions,
        allow_zero_collateral_balance=allow_zero_collateral_balance,
    )
    if total_capital_usd <= 0:
        return {
            "status": "SKIPPED",
            "reason": "total budget capital is zero",
            "total_capital_usd": 0.0,
        }
    return {
        "status": "OK",
        **refresh_active_budgets_after_exit_reserve(
            total_capital_usd=total_capital_usd,
            open_positions=open_positions,
        ),
    }
