from __future__ import annotations

from dataclasses import asdict
from typing import Any, Callable

from execution.copy_worker import LeaderSignal, process_signal
from execution.leader_signal_source import (
    CopyableSignalCandidate,
    fresh_copyable_signals_from_wallet,
)
from execution.signal_observation_store import log_signal_observation


SignalFetcher = Callable[..., tuple[LeaderSignal | None, dict[str, Any] | None, dict[str, Any]]]
MultiSignalFetcher = Callable[..., tuple[list[CopyableSignalCandidate], dict[str, Any]]]
SignalProcessor = Callable[[LeaderSignal], dict[str, Any]]
ObservationLogger = Callable[..., None]


def filter_registry_rows_for_scan(
    *,
    registry_rows: list[dict[str, Any]],
    open_positions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    open_position_wallets = {
        str(row.get("leader_wallet") or "")
        for row in open_positions
        if row.get("leader_wallet")
    }

    rows: list[dict[str, Any]] = []
    for row in registry_rows:
        wallet = str(row.get("wallet") or "")
        leader_status = str(row.get("leader_status") or "").upper()
        if leader_status == "EXIT_ONLY" and wallet not in open_position_wallets:
            continue
        rows.append(row)

    return rows


def _safe_float(value: Any) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _snapshot_float(snapshot: dict[str, Any] | None, key: str) -> float | None:
    if not snapshot:
        return None
    return _safe_float(snapshot.get(key))


def _snapshot_min_order_usd(snapshot: dict[str, Any] | None) -> float | None:
    min_order_size = _snapshot_float(snapshot, "min_order_size")
    if min_order_size is None or min_order_size <= 0:
        return None
    side = str((snapshot or {}).get("side") or "").upper()
    if side == "BUY":
        price_quote = _snapshot_float(snapshot, "best_ask")
    elif side == "SELL":
        price_quote = _snapshot_float(snapshot, "best_bid")
    else:
        price_quote = None
    if price_quote is None or price_quote <= 0:
        price_quote = _snapshot_float(snapshot, "price_quote")
    if price_quote is None or price_quote <= 0:
        price_quote = _snapshot_float(snapshot, "midpoint")
    if price_quote is None or price_quote <= 0:
        return None
    return round(min_order_size * price_quote, 8)


def _log_observation(
    *,
    row: dict[str, Any],
    signal: LeaderSignal | None,
    snapshot: dict[str, Any] | None,
    summary: dict[str, Any],
    observation_logger: ObservationLogger,
) -> None:
    observation_logger(
        leader_wallet=str(row["wallet"]),
        leader_user_name=row.get("user_name"),
        category=row.get("category"),
        leader_status=row.get("leader_status"),
        target_budget_usd=_safe_float(row.get("target_budget_usd")) or 0.0,
        latest_trade_side=summary.get("latest_trade_side"),
        latest_trade_age_sec=_safe_float(summary.get("latest_trade_age_sec")),
        latest_trade_hash=summary.get("latest_trade_hash"),
        latest_status=summary.get("latest_status"),
        latest_reason=summary.get("latest_reason"),
        selected_signal_id=signal.signal_id if signal else None,
        selected_side=signal.side if signal else None,
        token_id=signal.token_id if signal else None,
        selected_trade_age_sec=_safe_float(summary.get("selected_trade_age_sec")),
        selected_trade_notional_usd=_safe_float(summary.get("selected_trade_notional_usd")),
        selected_leader_portfolio_value_usd=_safe_float(
            summary.get("selected_leader_portfolio_value_usd")
        ),
        selected_leader_token_position_size=_safe_float(
            summary.get("selected_leader_token_position_size")
        ),
        selected_leader_token_position_value_usd=_safe_float(
            summary.get("selected_leader_token_position_value_usd")
        ),
        selected_leader_exit_fraction=_safe_float(summary.get("selected_leader_exit_fraction")),
        selected_leader_position_context_error=summary.get(
            "selected_leader_position_context_error"
        ),
        snapshot_midpoint=_snapshot_float(snapshot, "midpoint"),
        snapshot_best_bid=_snapshot_float(snapshot, "best_bid"),
        snapshot_best_ask=_snapshot_float(snapshot, "best_ask"),
        snapshot_spread=_snapshot_float(snapshot, "spread"),
        snapshot_min_order_size=_snapshot_float(snapshot, "min_order_size"),
        snapshot_min_order_usd=_snapshot_min_order_usd(snapshot),
        latest_token_id=summary.get("latest_token_id"),
        latest_trade_price=_safe_float(summary.get("latest_trade_price")),
        latest_snapshot_midpoint=_safe_float(summary.get("latest_snapshot_midpoint")),
        latest_snapshot_best_bid=_safe_float(summary.get("latest_snapshot_best_bid")),
        latest_snapshot_best_ask=_safe_float(summary.get("latest_snapshot_best_ask")),
        latest_snapshot_spread=_safe_float(summary.get("latest_snapshot_spread")),
        latest_snapshot_min_order_size=_safe_float(
            summary.get("latest_snapshot_min_order_size")
        ),
        latest_snapshot_min_order_usd=_safe_float(summary.get("latest_snapshot_min_order_usd")),
    )


def run_soak_cycle(
    *,
    registry_rows: list[dict[str, Any]],
    signal_fetcher: SignalFetcher | None = None,
    multi_signal_fetcher: MultiSignalFetcher = fresh_copyable_signals_from_wallet,
    signal_processor: SignalProcessor = process_signal,
    observation_logger: ObservationLogger = log_signal_observation,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for idx, registry_row in enumerate(registry_rows, start=1):
        wallet = str(registry_row["wallet"])
        user_name = registry_row.get("user_name")
        category = registry_row.get("category")
        leader_status = registry_row.get("leader_status")
        target_budget_usd = _safe_float(registry_row.get("target_budget_usd")) or 0.0

        base = {
            "idx": idx,
            "wallet": wallet,
            "user_name": user_name,
            "category": category,
            "leader_status": leader_status,
            "target_budget_usd": target_budget_usd,
        }

        try:
            if signal_fetcher is not None:
                signal, snapshot, summary = signal_fetcher(
                    wallet=wallet,
                    leader_budget_usd=target_budget_usd,
                )
                candidates = (
                    [
                        CopyableSignalCandidate(
                            signal=signal,
                            snapshot=snapshot or {},
                            summary=summary,
                        )
                    ]
                    if signal
                    else []
                )
            else:
                candidates, summary = multi_signal_fetcher(
                    wallet=wallet,
                    leader_budget_usd=target_budget_usd,
                )
        except Exception as e:
            summary = {
                "latest_status": "SOURCE_ERROR",
                "latest_reason": str(e),
                "latest_trade_side": None,
                "latest_trade_age_sec": None,
                "latest_trade_hash": None,
                "selected_trade_age_sec": None,
                "selected_trade_notional_usd": None,
                "selected_leader_portfolio_value_usd": None,
                "selected_leader_token_position_size": None,
                "selected_leader_token_position_value_usd": None,
                "selected_leader_exit_fraction": None,
                "selected_leader_position_context_error": None,
            }
            _log_observation(
                row=registry_row,
                signal=None,
                snapshot=None,
                summary=summary,
                observation_logger=observation_logger,
            )
            rows.append(
                {
                    **base,
                    "latest_status": "SOURCE_ERROR",
                    "latest_reason": str(e),
                    "selected_signal_id": None,
                    "selected_side": None,
                    "process_status": "SOURCE_ERROR",
                    "process_reason": str(e),
                }
            )
            continue

        if not candidates:
            _log_observation(
                row=registry_row,
                signal=None,
                snapshot=None,
                summary=summary,
                observation_logger=observation_logger,
            )
            rows.append(
                {
                    **base,
                    "latest_status": summary.get("latest_status"),
                    "latest_reason": summary.get("latest_reason"),
                    "selected_signal_id": None,
                    "selected_side": None,
                    "process_status": "NO_SIGNAL",
                    "process_reason": summary.get("latest_reason"),
                }
            )
            continue

        for candidate in reversed(candidates):
            signal = candidate.signal
            snapshot = candidate.snapshot
            selected_summary = candidate.summary

            _log_observation(
                row=registry_row,
                signal=signal,
                snapshot=snapshot,
                summary=selected_summary,
                observation_logger=observation_logger,
            )

            try:
                process_result = signal_processor(signal)
                process_status = process_result.get("status")
                process_reason = process_result.get("reason")
            except Exception as e:
                process_result = {"status": "PROCESS_ERROR", "reason": str(e)}
                process_status = "PROCESS_ERROR"
                process_reason = str(e)

            rows.append(
                {
                    **base,
                    "latest_status": selected_summary.get("latest_status"),
                    "latest_reason": selected_summary.get("latest_reason"),
                    "selected_signal_id": signal.signal_id,
                    "selected_side": signal.side,
                    "selected_trade_notional_usd": _safe_float(
                        selected_summary.get("selected_trade_notional_usd")
                    ),
                    "selected_leader_portfolio_value_usd": _safe_float(
                        selected_summary.get("selected_leader_portfolio_value_usd")
                    ),
                    "selected_leader_exit_fraction": _safe_float(
                        selected_summary.get("selected_leader_exit_fraction")
                    ),
                    "process_status": process_status,
                    "process_reason": process_reason,
                    "signal": asdict(signal),
                    "process_result": process_result,
                }
            )

    return rows


def summarize_soak_cycle(rows: list[dict[str, Any]]) -> dict[str, Any]:
    latest_status_counts: dict[str, int] = {}
    process_status_counts: dict[str, int] = {}

    for row in rows:
        latest_status = str(row.get("latest_status") or "UNKNOWN")
        process_status = str(row.get("process_status") or "UNKNOWN")
        latest_status_counts[latest_status] = latest_status_counts.get(latest_status, 0) + 1
        process_status_counts[process_status] = process_status_counts.get(process_status, 0) + 1

    leaders_checked = len(
        {
            (row.get("idx"), row.get("wallet"))
            for row in rows
            if row.get("idx") is not None and row.get("wallet")
        }
    )

    return {
        "leaders_checked": leaders_checked,
        "selected_signals": sum(1 for row in rows if row.get("selected_signal_id")),
        "latest_status_counts": latest_status_counts,
        "process_status_counts": process_status_counts,
    }
