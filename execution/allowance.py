from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams

from execution.polymarket_executor import build_authenticated_client


@dataclass(frozen=True)
class FundingSnapshot:
    balance_usd: float
    allowance_usd: float
    raw_response: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PreflightDecision:
    allowed: bool
    reason: str
    snapshot: FundingSnapshot | None = None
    details: dict[str, Any] = field(default_factory=dict)


def _safe_bool(value: Any, default: bool) -> bool:
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


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _decode_clob_amount(value: Any, *, decimals: int) -> float | None:
    if value is None:
        return None

    if isinstance(value, str) and "." in value:
        return _safe_float(value)

    raw = _safe_float(value)
    if raw is None:
        return None

    return raw / (10 ** decimals)


def parse_balance_allowance_response(
    raw: dict[str, Any],
    *,
    decimals: int = 6,
) -> FundingSnapshot:
    balance = _decode_clob_amount(raw.get("balance"), decimals=decimals)
    allowance = _decode_clob_amount(raw.get("allowance"), decimals=decimals)

    if balance is None:
        balance = _decode_clob_amount(raw.get("available_balance"), decimals=decimals)
    if allowance is None:
        allowance = _decode_clob_amount(raw.get("available_allowance"), decimals=decimals)

    if balance is None:
        balance = 0.0
    if allowance is None:
        allowance = 0.0

    return FundingSnapshot(
        balance_usd=round(balance, 8),
        allowance_usd=round(allowance, 8),
        raw_response=raw,
    )


def fetch_collateral_balance_allowance(config: dict[str, Any]) -> FundingSnapshot:
    funding_cfg = config.get("funding", {})
    decimals = int(funding_cfg.get("collateral_decimals", 6))

    client = build_authenticated_client()
    raw = client.get_balance_allowance(
        BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
    )
    if not isinstance(raw, dict):
        raw = {"raw": raw}

    return parse_balance_allowance_response(raw, decimals=decimals)


def evaluate_live_funding_preflight(
    *,
    config: dict[str, Any],
    side: str,
    amount_usd: float,
    snapshot_loader: Callable[[dict[str, Any]], FundingSnapshot] = fetch_collateral_balance_allowance,
) -> PreflightDecision:
    funding_cfg = config.get("funding", {})
    require_balance_allowance = _safe_bool(
        funding_cfg.get("require_balance_allowance"),
        True,
    )
    reserve_usd = float(funding_cfg.get("cash_reserve_usd", 0.0))
    reserve_pct = _safe_float(funding_cfg.get("cash_reserve_pct"))

    if not require_balance_allowance:
        return PreflightDecision(
            allowed=True,
            reason="balance/allowance preflight disabled by config",
            details={"required_usd": round(amount_usd + reserve_usd, 8)},
        )

    if side.upper() != "BUY":
        return PreflightDecision(
            allowed=True,
            reason="funding preflight currently applies to BUY collateral only",
            details={"side": side.upper()},
        )

    try:
        snapshot = snapshot_loader(config)
    except Exception as e:
        return PreflightDecision(
            allowed=False,
            reason=f"failed to fetch balance/allowance: {e}",
            details={"required_usd": round(amount_usd + reserve_usd, 8)},
        )

    if reserve_pct is not None and reserve_pct > 0:
        reserve_usd = round(snapshot.balance_usd * reserve_pct, 8)

    required_usd = amount_usd + reserve_usd
    details = {
        "amount_usd": round(amount_usd, 8),
        "cash_reserve_usd": round(reserve_usd, 8),
        "required_usd": round(required_usd, 8),
        "balance_usd": snapshot.balance_usd,
        "allowance_usd": snapshot.allowance_usd,
    }

    if snapshot.balance_usd + 1e-9 < required_usd:
        return PreflightDecision(
            allowed=False,
            reason=(
                f"insufficient collateral balance {snapshot.balance_usd:.2f} "
                f"for required {required_usd:.2f}"
            ),
            snapshot=snapshot,
            details=details,
        )

    if snapshot.allowance_usd + 1e-9 < amount_usd:
        return PreflightDecision(
            allowed=False,
            reason=(
                f"insufficient collateral allowance {snapshot.allowance_usd:.2f} "
                f"for amount {amount_usd:.2f}"
            ),
            snapshot=snapshot,
            details=details,
        )

    return PreflightDecision(
        allowed=True,
        reason="ok",
        snapshot=snapshot,
        details=details,
    )
