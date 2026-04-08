from __future__ import annotations

import csv
import sqlite3
from collections import defaultdict
from pathlib import Path
from pprint import pprint

from execution.state_store import DB_PATH
from execution.builder_auth import load_executor_config


OUT_EVENTS = Path("data/replay_signal_observations_events.csv")
OUT_SUMMARY_LEADER = Path("data/replay_signal_observations_by_leader.csv")
OUT_SUMMARY_CATEGORY = Path("data/replay_signal_observations_by_category.csv")

ALLOWED_REPLAY_STATUSES = {
    "FRESH_COPYABLE",
    "LATE_BUT_COPYABLE",
    "EXIT_FOLLOW",
    "EXIT_FOLLOW_STALE",
}


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def list_signal_observations(limit: int = 100000) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM signal_observations
        ORDER BY observation_id ASC
        LIMIT ?
        """,
        (limit,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def _safe_float(x):
    try:
        return float(x) if x is not None else 0.0
    except Exception:
        return 0.0


def _compute_copy_amount(
    *,
    selected_trade_notional_usd: float,
    target_budget_usd: float,
    leader_trade_notional_copy_fraction: float,
    min_order_size_usd: float,
    max_per_trade_usd: float,
) -> tuple[float, str]:
    if selected_trade_notional_usd > 0:
        amount = selected_trade_notional_usd * leader_trade_notional_copy_fraction
        amount = max(min_order_size_usd, amount)
        amount = min(max_per_trade_usd, amount, target_budget_usd)
        return round(amount, 6), "leader_trade_notional"

    fallback = min(max_per_trade_usd, target_budget_usd)
    fallback = max(min_order_size_usd, fallback)
    return round(fallback, 6), "fallback_budget"


def save_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        print(f"No rows to save for {path}")
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved: {path}")


def summarize(events: list[dict], key_field: str) -> list[dict]:
    grouped = defaultdict(list)
    for event in events:
        grouped[event.get(key_field) or "UNKNOWN"].append(event)

    rows = []
    for key, items in grouped.items():
        entries = [e for e in items if e["replay_event_type"] == "ENTRY"]
        exits = [e for e in items if e["replay_event_type"] == "EXIT"]

        realized = sum(_safe_float(e.get("realized_pnl_usd")) for e in exits)
        open_positions = sum(
            1 for e in items
            if e["is_final_state_row"] and _safe_float(e.get("position_after_usd")) > 0
        )

        rows.append(
            {
                key_field: key,
                "events": len(items),
                "entries": len(entries),
                "exits": len(exits),
                "realized_pnl_usd": round(realized, 6),
                "open_positions": open_positions,
            }
        )

    rows.sort(key=lambda r: (r["realized_pnl_usd"], r["entries"]), reverse=True)
    return rows


def main() -> None:
    cfg = load_executor_config()
    risk = cfg.get("risk", {})
    sizing = cfg.get("sizing", {})

    min_order_size_usd = float(risk.get("min_order_size_usd", 1.0))
    max_per_trade_usd = float(risk.get("max_per_trade_usd", 2.0))
    leader_trade_notional_copy_fraction = float(
        sizing.get("leader_trade_notional_copy_fraction", 0.20)
    )

    observations = list_signal_observations(limit=100000)
    if not observations:
        print("No signal observations yet.")
        return

    positions: dict[tuple[str, str], dict] = {}
    seen_signals: set[str] = set()
    replay_events: list[dict] = []
    skipped_rows: list[dict] = []

    for obs in observations:
        latest_status = obs.get("latest_status")
        selected_signal_id = obs.get("selected_signal_id")
        selected_side = obs.get("selected_side")
        token_id = obs.get("token_id")
        leader_wallet = obs.get("leader_wallet")
        leader_user_name = obs.get("leader_user_name")
        category = obs.get("category")

        if latest_status not in ALLOWED_REPLAY_STATUSES:
            skipped_rows.append(
                {
                    "observation_id": obs.get("observation_id"),
                    "leader_user_name": leader_user_name,
                    "category": category,
                    "latest_status": latest_status,
                    "skip_reason": "latest_status not replayable",
                }
            )
            continue

        if not selected_signal_id or not selected_side or not token_id or not leader_wallet:
            skipped_rows.append(
                {
                    "observation_id": obs.get("observation_id"),
                    "leader_user_name": leader_user_name,
                    "category": category,
                    "latest_status": latest_status,
                    "skip_reason": "missing selected signal fields",
                }
            )
            continue

        if selected_signal_id in seen_signals:
            skipped_rows.append(
                {
                    "observation_id": obs.get("observation_id"),
                    "leader_user_name": leader_user_name,
                    "category": category,
                    "latest_status": latest_status,
                    "skip_reason": "duplicate selected_signal_id",
                }
            )
            continue
        seen_signals.add(selected_signal_id)

        snapshot_midpoint = _safe_float(obs.get("snapshot_midpoint"))
        snapshot_best_bid = _safe_float(obs.get("snapshot_best_bid"))
        selected_trade_notional_usd = _safe_float(obs.get("selected_trade_notional_usd"))
        target_budget_usd = _safe_float(obs.get("target_budget_usd"))

        amount_usd, sizing_source = _compute_copy_amount(
            selected_trade_notional_usd=selected_trade_notional_usd,
            target_budget_usd=target_budget_usd,
            leader_trade_notional_copy_fraction=leader_trade_notional_copy_fraction,
            min_order_size_usd=min_order_size_usd,
            max_per_trade_usd=max_per_trade_usd,
        )

        key = (leader_wallet, token_id)
        pos = positions.get(
            key,
            {
                "leader_wallet": leader_wallet,
                "leader_user_name": leader_user_name,
                "category": category,
                "token_id": token_id,
                "position_usd": 0.0,
                "avg_entry_price": None,
                "realized_pnl_usd": 0.0,
            },
        )

        position_before = _safe_float(pos["position_usd"])
        avg_entry_before = pos["avg_entry_price"]

        if selected_side == "BUY":
            exec_price = snapshot_midpoint
            if exec_price <= 0:
                skipped_rows.append(
                    {
                        "observation_id": obs.get("observation_id"),
                        "leader_user_name": leader_user_name,
                        "category": category,
                        "latest_status": latest_status,
                        "skip_reason": "invalid buy exec_price",
                    }
                )
                continue

            position_after = position_before + amount_usd

            if avg_entry_before is None or position_before <= 0:
                avg_entry_after = exec_price
            else:
                avg_entry_after = (
                    (position_before * avg_entry_before) + (amount_usd * exec_price)
                ) / position_after

            pos["position_usd"] = position_after
            pos["avg_entry_price"] = avg_entry_after

            replay_events.append(
                {
                    "observation_id": obs["observation_id"],
                    "observed_at": obs["observed_at"],
                    "leader_wallet": leader_wallet,
                    "leader_user_name": leader_user_name,
                    "category": category,
                    "token_id": token_id,
                    "source_latest_status": latest_status,
                    "selected_signal_id": selected_signal_id,
                    "selected_side": selected_side,
                    "selected_trade_notional_usd": selected_trade_notional_usd,
                    "sizing_source": sizing_source,
                    "amount_usd": amount_usd,
                    "exec_price": exec_price,
                    "position_before_usd": round(position_before, 6),
                    "position_after_usd": round(position_after, 6),
                    "avg_entry_after": round(avg_entry_after, 6),
                    "replay_event_type": "ENTRY",
                    "realized_pnl_usd": "",
                    "is_final_state_row": False,
                }
            )

        elif selected_side == "SELL":
            if position_before <= 0:
                skipped_rows.append(
                    {
                        "observation_id": obs.get("observation_id"),
                        "leader_user_name": leader_user_name,
                        "category": category,
                        "latest_status": latest_status,
                        "skip_reason": "sell without open replay position",
                    }
                )
                continue

            exec_price = snapshot_best_bid if snapshot_best_bid > 0 else snapshot_midpoint
            if exec_price <= 0:
                skipped_rows.append(
                    {
                        "observation_id": obs.get("observation_id"),
                        "leader_user_name": leader_user_name,
                        "category": category,
                        "latest_status": latest_status,
                        "skip_reason": "invalid sell exec_price",
                    }
                )
                continue

            sell_amount = min(position_before, amount_usd)
            position_after = position_before - sell_amount

            realized_pnl_usd = ""
            if avg_entry_before is not None and avg_entry_before > 0:
                realized_pnl_usd = round(
                    sell_amount * ((exec_price - avg_entry_before) / avg_entry_before),
                    6,
                )
                pos["realized_pnl_usd"] = _safe_float(pos["realized_pnl_usd"]) + float(realized_pnl_usd)

            pos["position_usd"] = max(position_after, 0.0)
            if pos["position_usd"] == 0:
                pos["avg_entry_price"] = None

            replay_events.append(
                {
                    "observation_id": obs["observation_id"],
                    "observed_at": obs["observed_at"],
                    "leader_wallet": leader_wallet,
                    "leader_user_name": leader_user_name,
                    "category": category,
                    "token_id": token_id,
                    "source_latest_status": latest_status,
                    "selected_signal_id": selected_signal_id,
                    "selected_side": selected_side,
                    "selected_trade_notional_usd": selected_trade_notional_usd,
                    "sizing_source": sizing_source,
                    "amount_usd": round(sell_amount, 6),
                    "exec_price": exec_price,
                    "position_before_usd": round(position_before, 6),
                    "position_after_usd": round(position_after, 6),
                    "avg_entry_after": round(pos["avg_entry_price"], 6) if pos["avg_entry_price"] is not None else "",
                    "replay_event_type": "EXIT",
                    "realized_pnl_usd": realized_pnl_usd,
                    "is_final_state_row": False,
                }
            )

        positions[key] = pos

    for (leader_wallet, token_id), pos in positions.items():
        replay_events.append(
            {
                "observation_id": "",
                "observed_at": "",
                "leader_wallet": leader_wallet,
                "leader_user_name": pos["leader_user_name"],
                "category": pos["category"],
                "token_id": token_id,
                "source_latest_status": "",
                "selected_signal_id": "",
                "selected_side": "",
                "selected_trade_notional_usd": "",
                "sizing_source": "",
                "amount_usd": "",
                "exec_price": "",
                "position_before_usd": "",
                "position_after_usd": round(_safe_float(pos["position_usd"]), 6),
                "avg_entry_after": round(pos["avg_entry_price"], 6) if pos["avg_entry_price"] is not None else "",
                "replay_event_type": "FINAL_STATE",
                "realized_pnl_usd": round(_safe_float(pos["realized_pnl_usd"]), 6),
                "is_final_state_row": True,
            }
        )

    by_leader = summarize(replay_events, "leader_user_name")
    by_category = summarize(replay_events, "category")

    print("=== REPLAY SIGNAL OBSERVATIONS | EVENTS ===")
    pprint(replay_events[:20])

    print("\n=== REPLAY SIGNAL OBSERVATIONS | BY LEADER ===")
    pprint(by_leader)

    print("\n=== REPLAY SIGNAL OBSERVATIONS | BY CATEGORY ===")
    pprint(by_category)

    print("\n=== REPLAY SIGNAL OBSERVATIONS | SKIPPED ===")
    pprint(skipped_rows[:50])

    save_csv(replay_events, OUT_EVENTS)
    save_csv(by_leader, OUT_SUMMARY_LEADER)
    save_csv(by_category, OUT_SUMMARY_CATEGORY)


if __name__ == "__main__":
    main()
