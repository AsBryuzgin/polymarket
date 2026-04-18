from __future__ import annotations

from typing import Any

import execution.state_store as state_store


def get_connection():
    return state_store.get_connection()


def init_signal_observation_table() -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS signal_observations (
            observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
            observed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            leader_wallet TEXT NOT NULL,
            leader_user_name TEXT,
            category TEXT,
            leader_status TEXT,
            target_budget_usd REAL,
            latest_trade_side TEXT,
            latest_trade_age_sec REAL,
            latest_trade_hash TEXT,
            latest_status TEXT,
            latest_reason TEXT,
            selected_signal_id TEXT,
            selected_side TEXT,
            token_id TEXT,
            selected_trade_age_sec REAL,
            selected_trade_notional_usd REAL,
            snapshot_midpoint REAL,
            snapshot_best_bid REAL,
            snapshot_best_ask REAL,
            snapshot_spread REAL
        )
        """
    )

    conn.commit()
    conn.close()


def log_signal_observation(
    *,
    leader_wallet: str,
    leader_user_name: str | None,
    category: str | None,
    leader_status: str | None,
    target_budget_usd: float | None,
    latest_trade_side: str | None,
    latest_trade_age_sec: float | None,
    latest_trade_hash: str | None,
    latest_status: str | None,
    latest_reason: str | None,
    selected_signal_id: str | None,
    selected_side: str | None,
    token_id: str | None,
    selected_trade_age_sec: float | None,
    selected_trade_notional_usd: float | None,
    snapshot_midpoint: float | None,
    snapshot_best_bid: float | None,
    snapshot_best_ask: float | None,
    snapshot_spread: float | None,
) -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO signal_observations (
            leader_wallet,
            leader_user_name,
            category,
            leader_status,
            target_budget_usd,
            latest_trade_side,
            latest_trade_age_sec,
            latest_trade_hash,
            latest_status,
            latest_reason,
            selected_signal_id,
            selected_side,
            token_id,
            selected_trade_age_sec,
            selected_trade_notional_usd,
            snapshot_midpoint,
            snapshot_best_bid,
            snapshot_best_ask,
            snapshot_spread
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            leader_wallet,
            leader_user_name,
            category,
            leader_status,
            target_budget_usd,
            latest_trade_side,
            latest_trade_age_sec,
            latest_trade_hash,
            latest_status,
            latest_reason,
            selected_signal_id,
            selected_side,
            token_id,
            selected_trade_age_sec,
            selected_trade_notional_usd,
            snapshot_midpoint,
            snapshot_best_bid,
            snapshot_best_ask,
            snapshot_spread,
        ),
    )

    conn.commit()
    conn.close()


def list_signal_observations(limit: int = 200) -> list[dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT *
        FROM signal_observations
        ORDER BY observation_id DESC
        LIMIT ?
        """,
        (limit,),
    )

    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows
