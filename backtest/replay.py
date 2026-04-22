from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from backtest.metrics import compute_replay_metrics, summarize_replay_events
from backtest.simulator import safe_float, simulate_position_fills
from risk.sizing import compute_signal_copy_amount


ALLOWED_REPLAY_STATUSES = {
    "FRESH_COPYABLE",
    "LATE_BUT_COPYABLE",
    "EXIT_FOLLOW",
    "EXIT_FOLLOW_STALE",
}


@dataclass(frozen=True)
class SignalObservationReplayReport:
    event_rows: list[dict[str, Any]]
    skipped_rows: list[dict[str, Any]]
    by_leader: list[dict[str, Any]]
    by_category: list[dict[str, Any]]
    metrics: dict[str, Any]


def _skip_row(obs: dict[str, Any], reason: str) -> dict[str, Any]:
    return {
        "observation_id": obs.get("observation_id"),
        "leader_user_name": obs.get("leader_user_name"),
        "category": obs.get("category"),
        "latest_status": obs.get("latest_status"),
        "skip_reason": reason,
    }


def build_fills_from_signal_observations(
    observation_rows: list[dict[str, Any]],
    *,
    leader_trade_notional_copy_fraction: float,
    min_order_size_usd: float,
    max_per_trade_usd: float,
    max_leader_trade_budget_fraction: float | None = None,
    allow_notional_fallback: bool = False,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    fill_rows: list[dict[str, Any]] = []
    skipped_rows: list[dict[str, Any]] = []
    seen_signals: set[str] = set()
    leader_exposure: dict[str, float] = {}
    position_exposure: dict[tuple[str, str], float] = {}

    for obs in observation_rows:
        latest_status = obs.get("latest_status")
        selected_signal_id = obs.get("selected_signal_id")
        selected_side = str(obs.get("selected_side") or "").upper()
        token_id = obs.get("token_id")
        leader_wallet = obs.get("leader_wallet")

        if latest_status not in ALLOWED_REPLAY_STATUSES:
            skipped_rows.append(_skip_row(obs, "latest_status not replayable"))
            continue

        if not selected_signal_id or not selected_side or not token_id or not leader_wallet:
            skipped_rows.append(_skip_row(obs, "missing selected signal fields"))
            continue

        if selected_signal_id in seen_signals:
            skipped_rows.append(_skip_row(obs, "duplicate selected_signal_id"))
            continue
        seen_signals.add(str(selected_signal_id))

        selected_trade_notional_usd = safe_float(obs.get("selected_trade_notional_usd"))
        target_budget_usd = safe_float(obs.get("target_budget_usd"))
        position_key = (str(leader_wallet), str(token_id))

        if selected_side == "BUY":
            sizing_budget_usd = target_budget_usd
            remaining_budget_usd = max(
                target_budget_usd - leader_exposure.get(str(leader_wallet), 0.0),
                0.0,
            )
        elif selected_side == "SELL":
            sizing_budget_usd = position_exposure.get(position_key, 0.0)
            remaining_budget_usd = sizing_budget_usd
            if sizing_budget_usd <= 0:
                skipped_rows.append(_skip_row(obs, "sell signal but no replay open position"))
                continue
        else:
            skipped_rows.append(_skip_row(obs, f"unsupported selected side: {selected_side}"))
            continue

        size_decision = compute_signal_copy_amount(
            leader_budget_usd=sizing_budget_usd,
            remaining_leader_budget_usd=remaining_budget_usd,
            leader_trade_notional_usd=selected_trade_notional_usd,
            leader_trade_notional_copy_fraction=leader_trade_notional_copy_fraction,
            min_order_size_usd=min_order_size_usd,
            max_per_trade_usd=max_per_trade_usd,
            side=selected_side,
            leader_portfolio_value_usd=safe_float(
                obs.get("selected_leader_portfolio_value_usd")
            ),
            leader_exit_fraction=safe_float(obs.get("selected_leader_exit_fraction")),
            max_leader_trade_budget_fraction=max_leader_trade_budget_fraction,
            allow_notional_fallback=allow_notional_fallback,
            precision=6,
        )

        if not size_decision.allowed:
            skipped_rows.append(_skip_row(obs, f"sizing blocked: {size_decision.reason}"))
            continue

        snapshot_midpoint = safe_float(obs.get("snapshot_midpoint"))
        snapshot_best_bid = safe_float(obs.get("snapshot_best_bid"))
        if selected_side == "BUY":
            exec_price = snapshot_midpoint
        elif selected_side == "SELL":
            exec_price = snapshot_best_bid if snapshot_best_bid > 0 else snapshot_midpoint

        if exec_price <= 0:
            skipped_rows.append(_skip_row(obs, "invalid exec_price"))
            continue

        amount_usd = size_decision.amount_usd
        if selected_side == "SELL":
            amount_usd = min(amount_usd, position_exposure.get(position_key, 0.0))

        if selected_side == "BUY":
            leader_exposure[str(leader_wallet)] = (
                leader_exposure.get(str(leader_wallet), 0.0) + amount_usd
            )
            position_exposure[position_key] = position_exposure.get(position_key, 0.0) + amount_usd
        else:
            position_exposure[position_key] = max(
                position_exposure.get(position_key, 0.0) - amount_usd,
                0.0,
            )
            leader_exposure[str(leader_wallet)] = max(
                leader_exposure.get(str(leader_wallet), 0.0) - amount_usd,
                0.0,
            )

        fill_rows.append(
            {
                "observation_id": obs.get("observation_id"),
                "observed_at": obs.get("observed_at"),
                "leader_wallet": leader_wallet,
                "leader_user_name": obs.get("leader_user_name"),
                "category": obs.get("category"),
                "token_id": token_id,
                "source_latest_status": latest_status,
                "selected_signal_id": selected_signal_id,
                "selected_trade_notional_usd": selected_trade_notional_usd,
                "selected_leader_portfolio_value_usd": safe_float(
                    obs.get("selected_leader_portfolio_value_usd")
                ),
                "selected_leader_exit_fraction": safe_float(
                    obs.get("selected_leader_exit_fraction")
                ),
                "side": selected_side,
                "amount_usd": amount_usd,
                "exec_price": exec_price,
                "sizing_source": size_decision.source,
            }
        )

    return fill_rows, skipped_rows


def replay_signal_observations(
    observation_rows: list[dict[str, Any]],
    *,
    leader_trade_notional_copy_fraction: float,
    min_order_size_usd: float,
    max_per_trade_usd: float,
    max_leader_trade_budget_fraction: float | None = None,
    allow_notional_fallback: bool = False,
) -> SignalObservationReplayReport:
    fill_rows, skipped_rows = build_fills_from_signal_observations(
        observation_rows,
        leader_trade_notional_copy_fraction=leader_trade_notional_copy_fraction,
        min_order_size_usd=min_order_size_usd,
        max_per_trade_usd=max_per_trade_usd,
        max_leader_trade_budget_fraction=max_leader_trade_budget_fraction,
        allow_notional_fallback=allow_notional_fallback,
    )
    simulation = simulate_position_fills(fill_rows)
    all_skipped = [*skipped_rows, *simulation.skipped_rows]

    return SignalObservationReplayReport(
        event_rows=simulation.event_rows,
        skipped_rows=all_skipped,
        by_leader=summarize_replay_events(simulation.event_rows, "leader_user_name"),
        by_category=summarize_replay_events(simulation.event_rows, "category"),
        metrics=compute_replay_metrics(simulation.event_rows),
    )
