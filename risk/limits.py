from __future__ import annotations

from dataclasses import dataclass
from typing import Any


def _float_or_default(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _positive_float_or_none(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


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


@dataclass(frozen=True)
class RiskLimits:
    min_order_size_usd: float = 1.0
    max_per_trade_usd: float = 2.0
    max_position_usd: float | None = None
    max_wallet_exposure_usd: float | None = None
    max_category_exposure_usd: float | None = None
    max_portfolio_exposure_usd: float | None = None
    max_daily_realized_loss_usd: float | None = None
    enforce_leader_budget_cap: bool = True
    trading_disabled: bool = False
    capital_base_usd: float | None = None
    capital_base_required: bool = False
    capital_base_error: str | None = None

    @property
    def capital_base_missing(self) -> bool:
        return self.capital_base_required and (
            self.capital_base_usd is None or self.capital_base_usd <= 0
        )

    @classmethod
    def from_config(
        cls,
        config: dict[str, Any],
        *,
        capital_base_usd: float | None = None,
        capital_base_error: str | None = None,
    ) -> "RiskLimits":
        risk = config.get("risk", {})
        pct_keys = {
            "max_per_trade_pct",
            "max_position_pct",
            "max_wallet_exposure_pct",
            "max_category_exposure_pct",
            "max_portfolio_exposure_pct",
            "max_daily_realized_loss_pct",
        }
        capital_base_required = any(
            _positive_float_or_none(risk.get(key)) is not None for key in pct_keys
        )

        def resolve_usd(
            absolute_key: str,
            pct_key: str,
            default: float | None = None,
        ) -> float | None:
            absolute = _positive_float_or_none(risk.get(absolute_key))
            if absolute is not None:
                return absolute

            pct = _positive_float_or_none(risk.get(pct_key))
            if pct is not None:
                if capital_base_usd is not None and capital_base_usd > 0:
                    return round(capital_base_usd * pct, 8)
                return 0.0 if default is not None else None

            return default

        return cls(
            min_order_size_usd=_float_or_default(risk.get("min_order_size_usd"), 1.0),
            max_per_trade_usd=resolve_usd(
                "max_per_trade_usd",
                "max_per_trade_pct",
                2.0,
            ) or 0.0,
            max_position_usd=resolve_usd("max_position_usd", "max_position_pct"),
            max_wallet_exposure_usd=resolve_usd(
                "max_wallet_exposure_usd",
                "max_wallet_exposure_pct",
            ),
            max_category_exposure_usd=resolve_usd(
                "max_category_exposure_usd",
                "max_category_exposure_pct",
            ),
            max_portfolio_exposure_usd=resolve_usd(
                "max_portfolio_exposure_usd",
                "max_portfolio_exposure_pct",
            ),
            max_daily_realized_loss_usd=resolve_usd(
                "max_daily_realized_loss_usd",
                "max_daily_realized_loss_pct",
            ),
            enforce_leader_budget_cap=_bool_or_default(
                risk.get("enforce_leader_budget_cap"),
                True,
            ),
            trading_disabled=_bool_or_default(risk.get("trading_disabled"), False),
            capital_base_usd=capital_base_usd,
            capital_base_required=capital_base_required,
            capital_base_error=capital_base_error,
        )
