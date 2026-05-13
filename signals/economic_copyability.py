from __future__ import annotations

import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from statistics import median
from typing import Any

import execution.state_store as state_store
from execution.signal_observation_store import init_signal_observation_table


DEFAULT_MIN_BUY_SIGNALS = 20
DEFAULT_MIN_EXECUTABLE_RATIO = 0.10
DEFAULT_MIN_BATCHABLE_RATIO = 0.35
DEFAULT_LOOKBACK_HOURS = 168.0
DEFAULT_BATCH_WINDOW_SEC = 30.0
DEFAULT_MIN_ORDER_USD = 1.0
DEFAULT_MAX_ROUND_UP_MULTIPLE = 3.0
DEFAULT_MIN_MEDIAN_TRADE_FRACTION = 0.001


@dataclass(frozen=True)
class EconomicCopyabilityMetrics:
    wallet: str
    buy_signals: int
    executable_now: int
    executable_with_roundup: int
    executable_after_batch: int
    dust_signals: int
    trade_fraction_samples: int
    median_trade_fraction: float
    mean_trade_fraction: float
    median_copy_amount_usd: float
    required_bankroll_p95_signals_usd: float
    required_bankroll_p99_signals_usd: float
    required_bankroll_p95_batch_usd: float
    required_bankroll_p99_batch_usd: float
    required_bankroll_p95_volume_usd: float
    required_bankroll_p99_volume_usd: float
    executable_ratio: float
    batchable_ratio: float
    dust_ratio: float
    status: str
    reason: str


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed


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


def _parse_ts(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    text = str(value).replace(" ", "T")
    try:
        return datetime.fromisoformat(text).replace(tzinfo=timezone.utc).timestamp()
    except ValueError:
        return 0.0


def _metric_cfg(config: dict[str, Any]) -> dict[str, Any]:
    cfg = config.get("economic_copyability", {})
    risk = config.get("risk", {})
    sizing = config.get("sizing", {})
    batch = config.get("signal_batch_coalescer", {})
    return {
        "enabled": _safe_bool(cfg.get("enabled"), True),
        "lookback_hours": _safe_float(cfg.get("lookback_hours"), DEFAULT_LOOKBACK_HOURS),
        "min_buy_signals": int(_safe_float(cfg.get("min_buy_signals"), DEFAULT_MIN_BUY_SIGNALS)),
        "min_executable_ratio": _safe_float(
            cfg.get("min_executable_ratio"),
            DEFAULT_MIN_EXECUTABLE_RATIO,
        ),
        "min_batchable_ratio": _safe_float(
            cfg.get("min_batchable_ratio"),
            DEFAULT_MIN_BATCHABLE_RATIO,
        ),
        "min_order_usd": _safe_float(risk.get("min_order_size_usd"), DEFAULT_MIN_ORDER_USD),
        "max_round_up_multiple": _safe_float(
            sizing.get("max_min_order_round_up_multiple"),
            DEFAULT_MAX_ROUND_UP_MULTIPLE,
        ),
        "batch_window_sec": _safe_float(batch.get("window_sec"), DEFAULT_BATCH_WINDOW_SEC),
        "min_median_trade_fraction": _safe_float(
            cfg.get("min_median_trade_fraction"),
            DEFAULT_MIN_MEDIAN_TRADE_FRACTION,
        ),
        "max_leader_trade_budget_fraction": _safe_float(
            sizing.get("max_leader_trade_budget_fraction"),
            0.0,
        ),
    }


def _leader_trade_fraction(row: dict[str, Any]) -> float:
    notional = _safe_float(row.get("selected_trade_notional_usd"))
    portfolio = _safe_float(row.get("selected_leader_portfolio_value_usd"))
    if notional <= 0 or portfolio <= 0:
        return 0.0
    return min(notional / portfolio, 1.0)


def _effective_trade_fraction(row: dict[str, Any], max_fraction: float) -> float:
    fraction = _leader_trade_fraction(row)
    if max_fraction > 0:
        return min(fraction, max_fraction)
    return fraction


def _copy_amount(row: dict[str, Any]) -> float:
    target_budget = _safe_float(row.get("target_budget_usd"))
    if target_budget <= 0:
        return 0.0
    return target_budget * _leader_trade_fraction(row)


def _weighted_quantile(values: list[float], weights: list[float], q: float) -> float:
    pairs = sorted(
        (value, weight)
        for value, weight in zip(values, weights)
        if value > 0 and weight > 0
    )
    if not pairs:
        return 0.0
    total = sum(weight for _value, weight in pairs)
    threshold = total * q
    running = 0.0
    for value, weight in pairs:
        running += weight
        if running >= threshold:
            return value
    return pairs[-1][0]


def _quantile(values: list[float], q: float) -> float:
    vals = sorted(value for value in values if value > 0)
    if not vals:
        return 0.0
    if len(vals) == 1:
        return vals[0]
    pos = (len(vals) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(vals) - 1)
    if lo == hi:
        return vals[lo]
    frac = pos - lo
    return vals[lo] * (1.0 - frac) + vals[hi] * frac


def _min_order(row: dict[str, Any], default: float) -> float:
    for key in ("snapshot_min_order_usd", "latest_snapshot_min_order_usd"):
        parsed = _safe_float(row.get(key))
        if parsed > 0:
            return parsed
    return default


def _load_buy_observations(lookback_hours: float) -> list[dict[str, Any]]:
    try:
        init_signal_observation_table()
        conn = state_store.get_connection()
        cur = conn.cursor()
    except sqlite3.Error:
        return []
    try:
        cur.execute(
            """
            SELECT *
            FROM signal_observations
            WHERE selected_side = 'BUY'
              AND selected_signal_id IS NOT NULL
              AND observed_at >= datetime('now', ?)
            ORDER BY observed_at ASC, observation_id ASC
            """,
            (f"-{float(lookback_hours):.3f} hours",),
        )
        rows = [dict(row) for row in cur.fetchall()]
    except sqlite3.Error:
        rows = []
    finally:
        conn.close()

    by_signal: dict[str, dict[str, Any]] = {}
    for row in rows:
        signal_id = str(row.get("selected_signal_id") or "")
        if signal_id and signal_id not in by_signal:
            by_signal[signal_id] = row
    return list(by_signal.values())


def _batchable_signal_ids(
    rows: list[dict[str, Any]],
    *,
    amounts: dict[str, float],
    min_orders: dict[str, float],
    batch_window_sec: float,
) -> set[str]:
    batchable: set[str] = set()
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        wallet = str(row.get("leader_wallet") or "").lower()
        token_id = str(row.get("token_id") or "")
        grouped.setdefault((wallet, token_id), []).append(row)

    for items in grouped.values():
        current: list[dict[str, Any]] = []
        current_amount = 0.0
        current_min_order = DEFAULT_MIN_ORDER_USD
        start_ts = 0.0

        for row in items:
            signal_id = str(row.get("selected_signal_id") or "")
            observed_ts = _parse_ts(row.get("observed_at"))
            amount = amounts.get(signal_id, 0.0)
            min_order = min_orders.get(signal_id, DEFAULT_MIN_ORDER_USD)
            if not current:
                current = [row]
                current_amount = amount
                current_min_order = min_order
                start_ts = observed_ts
            elif observed_ts - start_ts <= batch_window_sec:
                current.append(row)
                current_amount += amount
                current_min_order = max(current_min_order, min_order)
            else:
                if current_amount >= current_min_order:
                    batchable.update(str(item.get("selected_signal_id") or "") for item in current)
                current = [row]
                current_amount = amount
                current_min_order = min_order
                start_ts = observed_ts

        if current and current_amount >= current_min_order:
            batchable.update(str(item.get("selected_signal_id") or "") for item in current)

    return {signal_id for signal_id in batchable if signal_id}


def _batch_requirement_pairs(
    rows: list[dict[str, Any]],
    *,
    batch_window_sec: float,
    default_min_order_usd: float,
    max_trade_fraction: float,
) -> list[tuple[float, float]]:
    pairs: list[tuple[float, float]] = []
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        wallet = str(row.get("leader_wallet") or "").lower()
        token_id = str(row.get("token_id") or "")
        grouped.setdefault((wallet, token_id), []).append(row)

    for items in grouped.values():
        current: list[dict[str, Any]] = []
        start_ts = 0.0

        def flush(batch: list[dict[str, Any]]) -> None:
            if not batch:
                return
            fraction_sum = sum(
                _effective_trade_fraction(item, max_trade_fraction)
                for item in batch
            )
            if fraction_sum <= 0:
                return
            min_order = max(
                _min_order(item, default_min_order_usd)
                for item in batch
            )
            pairs.append((min_order / fraction_sum, fraction_sum))

        for row in items:
            observed_ts = _parse_ts(row.get("observed_at"))
            if not current:
                current = [row]
                start_ts = observed_ts
            elif observed_ts - start_ts <= batch_window_sec:
                current.append(row)
            else:
                flush(current)
                current = [row]
                start_ts = observed_ts
        flush(current)

    return pairs


def _volume_coverage(requirements: list[tuple[float, float]], budget_usd: float) -> float:
    total_weight = sum(weight for _required, weight in requirements if weight > 0)
    if total_weight <= 0 or budget_usd <= 0:
        return 0.0
    covered = sum(
        weight
        for required, weight in requirements
        if weight > 0 and budget_usd + 1e-12 >= required
    )
    return covered / total_weight


def compute_economic_copyability_by_wallet(
    *,
    config: dict[str, Any],
) -> dict[str, EconomicCopyabilityMetrics]:
    cfg = _metric_cfg(config)
    if not cfg["enabled"]:
        return {}

    rows = _load_buy_observations(float(cfg["lookback_hours"]))
    by_wallet: dict[str, list[dict[str, Any]]] = {}
    amounts: dict[str, float] = {}
    min_orders: dict[str, float] = {}

    for row in rows:
        wallet = str(row.get("leader_wallet") or "").lower()
        signal_id = str(row.get("selected_signal_id") or "")
        if not wallet or not signal_id:
            continue
        trade_fraction = _effective_trade_fraction(
            row,
            float(cfg["max_leader_trade_budget_fraction"]),
        )
        amount = _safe_float(row.get("target_budget_usd")) * trade_fraction
        min_order = _min_order(row, float(cfg["min_order_usd"]))
        row["economic_copy_amount_usd"] = amount
        row["economic_trade_fraction"] = trade_fraction
        row["economic_min_order_usd"] = min_order
        by_wallet.setdefault(wallet, []).append(row)
        amounts[signal_id] = amount
        min_orders[signal_id] = min_order

    batchable_ids = _batchable_signal_ids(
        rows,
        amounts=amounts,
        min_orders=min_orders,
        batch_window_sec=float(cfg["batch_window_sec"]),
    )

    out: dict[str, EconomicCopyabilityMetrics] = {}
    for wallet, wallet_rows in by_wallet.items():
        buy_signals = len(wallet_rows)
        now_count = 0
        roundup_count = 0
        batch_count = 0
        copy_amounts: list[float] = []
        trade_fractions: list[float] = []
        signal_required_budgets: list[float] = []

        for row in wallet_rows:
            signal_id = str(row.get("selected_signal_id") or "")
            amount = float(row["economic_copy_amount_usd"])
            trade_fraction = float(row["economic_trade_fraction"])
            min_order = float(row["economic_min_order_usd"])
            copy_amounts.append(amount)
            if trade_fraction > 0:
                trade_fractions.append(trade_fraction)
                signal_required_budgets.append(min_order / trade_fraction)
            if amount >= min_order:
                now_count += 1
                batch_count += 1
                continue
            if amount > 0 and min_order / amount <= float(cfg["max_round_up_multiple"]):
                roundup_count += 1
                batch_count += 1
                continue
            if signal_id in batchable_ids:
                batch_count += 1

        dust_count = max(buy_signals - batch_count, 0)
        executable_count = now_count + roundup_count
        executable_ratio = executable_count / buy_signals if buy_signals else 0.0
        batchable_ratio = batch_count / buy_signals if buy_signals else 0.0
        dust_ratio = dust_count / buy_signals if buy_signals else 0.0
        median_trade_fraction = median(trade_fractions) if trade_fractions else 0.0
        mean_trade_fraction = (
            sum(trade_fractions) / len(trade_fractions) if trade_fractions else 0.0
        )
        batch_requirements = _batch_requirement_pairs(
            wallet_rows,
            batch_window_sec=float(cfg["batch_window_sec"]),
            default_min_order_usd=float(cfg["min_order_usd"]),
            max_trade_fraction=float(cfg["max_leader_trade_budget_fraction"]),
        )
        batch_required_budgets = [required for required, _weight in batch_requirements]
        batch_required_weights = [weight for _required, weight in batch_requirements]

        if buy_signals < int(cfg["min_buy_signals"]):
            status = "UNKNOWN"
            reason = f"insufficient runtime buy samples: {buy_signals}/{cfg['min_buy_signals']}"
        elif (
            executable_ratio < float(cfg["min_executable_ratio"])
            and batchable_ratio < float(cfg["min_batchable_ratio"])
        ):
            status = "FAIL"
            reason = (
                "runtime economic copyability below thresholds: "
                f"exec {executable_ratio:.2f} < {cfg['min_executable_ratio']:.2f}, "
                f"batch {batchable_ratio:.2f} < {cfg['min_batchable_ratio']:.2f}"
            )
        elif (
            len(trade_fractions) >= int(cfg["min_buy_signals"])
            and median_trade_fraction < float(cfg["min_median_trade_fraction"])
            and batchable_ratio < float(cfg["min_batchable_ratio"])
        ):
            status = "FAIL"
            reason = (
                "runtime leader trade fraction below threshold: "
                f"median {median_trade_fraction:.4%} < "
                f"{float(cfg['min_median_trade_fraction']):.4%}, "
                f"batch {batchable_ratio:.2f} < {cfg['min_batchable_ratio']:.2f}"
            )
        else:
            status = "PASS"
            reason = "runtime economic copyability ok"

        out[wallet] = EconomicCopyabilityMetrics(
            wallet=wallet,
            buy_signals=buy_signals,
            executable_now=now_count,
            executable_with_roundup=roundup_count,
            executable_after_batch=batch_count,
            dust_signals=dust_count,
            trade_fraction_samples=len(trade_fractions),
            median_trade_fraction=round(median_trade_fraction, 8),
            mean_trade_fraction=round(mean_trade_fraction, 8),
            median_copy_amount_usd=round(median(copy_amounts), 6) if copy_amounts else 0.0,
            required_bankroll_p95_signals_usd=round(_quantile(signal_required_budgets, 0.95), 2),
            required_bankroll_p99_signals_usd=round(_quantile(signal_required_budgets, 0.99), 2),
            required_bankroll_p95_batch_usd=round(_quantile(batch_required_budgets, 0.95), 2),
            required_bankroll_p99_batch_usd=round(_quantile(batch_required_budgets, 0.99), 2),
            required_bankroll_p95_volume_usd=round(
                _weighted_quantile(batch_required_budgets, batch_required_weights, 0.95),
                2,
            ),
            required_bankroll_p99_volume_usd=round(
                _weighted_quantile(batch_required_budgets, batch_required_weights, 0.99),
                2,
            ),
            executable_ratio=round(executable_ratio, 6),
            batchable_ratio=round(batchable_ratio, 6),
            dust_ratio=round(dust_ratio, 6),
            status=status,
            reason=reason,
        )
    return out


def compute_budget_volume_coverage_by_wallet(
    *,
    config: dict[str, Any],
    budget_by_wallet: dict[str, float],
) -> dict[str, dict[str, float]]:
    cfg = _metric_cfg(config)
    if not cfg["enabled"] or not budget_by_wallet:
        return {}

    rows = _load_buy_observations(float(cfg["lookback_hours"]))
    by_wallet: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        wallet = str(row.get("leader_wallet") or "").lower()
        if wallet in budget_by_wallet:
            by_wallet.setdefault(wallet, []).append(row)

    out: dict[str, dict[str, float]] = {}
    max_round_up_multiple = float(cfg["max_round_up_multiple"])
    for wallet, wallet_rows in by_wallet.items():
        requirements = _batch_requirement_pairs(
            wallet_rows,
            batch_window_sec=float(cfg["batch_window_sec"]),
            default_min_order_usd=float(cfg["min_order_usd"]),
            max_trade_fraction=float(cfg["max_leader_trade_budget_fraction"]),
        )
        budget = _safe_float(budget_by_wallet.get(wallet))
        if max_round_up_multiple > 0:
            roundup_requirements = [
                (required / max_round_up_multiple, weight)
                for required, weight in requirements
            ]
        else:
            roundup_requirements = requirements
        out[wallet] = {
            "budget_usd": round(budget, 2),
            "volume_coverage": round(_volume_coverage(requirements, budget), 6),
            "volume_coverage_with_roundup": round(
                _volume_coverage(roundup_requirements, budget),
                6,
            ),
        }
    return out


def annotate_rows_with_economic_copyability(
    rows: list[dict[str, Any]],
    *,
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    metrics_by_wallet = compute_economic_copyability_by_wallet(config=config)
    if not metrics_by_wallet:
        return rows

    for row in rows:
        wallet = str(row.get("wallet") or "").lower()
        metrics = metrics_by_wallet.get(wallet)
        if metrics is None:
            continue
        data = asdict(metrics)
        for key, value in data.items():
            if key == "wallet":
                continue
            row[f"economic_copyability_{key}"] = value
        if metrics.status == "FAIL":
            row["eligible"] = False
            existing_reason = str(row.get("filter_reasons") or "").strip()
            reason = f"economic_copyability: {metrics.reason}"
            row["filter_reasons"] = f"{existing_reason}; {reason}" if existing_reason else reason
    return rows
