from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


DB_PATH = Path("data/executor_state.db")


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_signals (
            signal_id TEXT PRIMARY KEY,
            leader_wallet TEXT NOT NULL,
            token_id TEXT NOT NULL,
            side TEXT NOT NULL,
            leader_budget_usd REAL NOT NULL,
            suggested_amount_usd REAL,
            status TEXT NOT NULL,
            reason TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    conn.commit()
    conn.close()


def has_signal(signal_id: str) -> bool:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        "SELECT 1 FROM processed_signals WHERE signal_id = ? LIMIT 1",
        (signal_id,),
    )
    row = cur.fetchone()
    conn.close()

    return row is not None


def record_signal(
    signal_id: str,
    leader_wallet: str,
    token_id: str,
    side: str,
    leader_budget_usd: float,
    suggested_amount_usd: float | None,
    status: str,
    reason: str | None,
) -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT OR REPLACE INTO processed_signals (
            signal_id,
            leader_wallet,
            token_id,
            side,
            leader_budget_usd,
            suggested_amount_usd,
            status,
            reason
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            signal_id,
            leader_wallet,
            token_id,
            side,
            leader_budget_usd,
            suggested_amount_usd,
            status,
            reason,
        ),
    )

    conn.commit()
    conn.close()


def list_recent_signals(limit: int = 20) -> list[dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT *
        FROM processed_signals
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (limit,),
    )

    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows
