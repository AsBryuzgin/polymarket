from __future__ import annotations

import sys
from pathlib import Path
from pprint import pprint

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.state_store import init_db, list_leader_registry
from execution.leader_signal_source import latest_fresh_copyable_signal_from_wallet
from execution.signal_observation_store import (
    init_signal_observation_table,
    log_signal_observation,
)


def _safe_float(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


def capture_once(verbose: bool = True) -> list[dict]:
    init_db()
    init_signal_observation_table()

    registry_rows = list_leader_registry(limit=100000)
    if not registry_rows:
        if verbose:
            print("No leader registry rows found.")
        return []

    rows_to_print = []

    for row in registry_rows:
        wallet = row["wallet"]
        user_name = row.get("user_name")
        category = row.get("category")
        leader_status = row.get("leader_status")
        target_budget_usd = _safe_float(row.get("target_budget_usd")) or 0.0

        signal, snapshot, summary = latest_fresh_copyable_signal_from_wallet(
            wallet=wallet,
            leader_budget_usd=target_budget_usd,
        )

        selected_signal_id = signal.signal_id if signal else None
        selected_side = signal.side if signal else None
        selected_token_id = signal.token_id if signal else None

        snapshot_midpoint = _safe_float(snapshot.get("midpoint")) if snapshot else None
        snapshot_best_bid = _safe_float(snapshot.get("best_bid")) if snapshot else None
        snapshot_best_ask = _safe_float(snapshot.get("best_ask")) if snapshot else None
        snapshot_spread = _safe_float(snapshot.get("spread")) if snapshot else None

        log_signal_observation(
            leader_wallet=wallet,
            leader_user_name=user_name,
            category=category,
            leader_status=leader_status,
            target_budget_usd=target_budget_usd,
            latest_trade_side=summary.get("latest_trade_side"),
            latest_trade_age_sec=_safe_float(summary.get("latest_trade_age_sec")),
            latest_trade_hash=summary.get("latest_trade_hash"),
            latest_status=summary.get("latest_status"),
            latest_reason=summary.get("latest_reason"),
            selected_signal_id=selected_signal_id,
            selected_side=selected_side,
            token_id=selected_token_id,
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
            snapshot_midpoint=snapshot_midpoint,
            snapshot_best_bid=snapshot_best_bid,
            snapshot_best_ask=snapshot_best_ask,
            snapshot_spread=snapshot_spread,
        )

        rows_to_print.append(
            {
                "category": category,
                "user_name": user_name,
                "wallet": wallet,
                "leader_status": leader_status,
                "latest_status": summary.get("latest_status"),
                "latest_reason": summary.get("latest_reason"),
                "selected_signal_id": selected_signal_id,
                "selected_side": selected_side,
                "token_id": selected_token_id,
                "selected_trade_notional_usd": _safe_float(summary.get("selected_trade_notional_usd")),
                "selected_leader_portfolio_value_usd": _safe_float(
                    summary.get("selected_leader_portfolio_value_usd")
                ),
                "selected_leader_exit_fraction": _safe_float(
                    summary.get("selected_leader_exit_fraction")
                ),
                "snapshot_spread": snapshot_spread,
            }
        )

    if verbose:
        print("=== SIGNAL SNAPSHOT CAPTURE ===")
        pprint(rows_to_print)
        print(f"\nSaved observations: {len(rows_to_print)}")

    return rows_to_print


def main() -> None:
    capture_once(verbose=True)


if __name__ == "__main__":
    main()
