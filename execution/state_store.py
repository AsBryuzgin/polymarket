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

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS copied_positions (
            leader_wallet TEXT NOT NULL,
            token_id TEXT NOT NULL,
            position_usd REAL NOT NULL,
            avg_entry_price REAL,
            status TEXT NOT NULL,
            last_signal_id TEXT,
            opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (leader_wallet, token_id)
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


def get_open_position(leader_wallet: str, token_id: str) -> dict[str, Any] | None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT *
        FROM copied_positions
        WHERE leader_wallet = ?
          AND token_id = ?
          AND status = 'OPEN'
        LIMIT 1
        """,
        (leader_wallet, token_id),
    )

    row = cur.fetchone()
    conn.close()

    return dict(row) if row else None


def upsert_buy_position(
    leader_wallet: str,
    token_id: str,
    amount_usd: float,
    entry_price: float | None,
    signal_id: str,
) -> None:
    existing = get_open_position(leader_wallet, token_id)

    conn = get_connection()
    cur = conn.cursor()

    if existing is None:
        cur.execute(
            """
            INSERT INTO copied_positions (
                leader_wallet,
                token_id,
                position_usd,
                avg_entry_price,
                status,
                last_signal_id
            ) VALUES (?, ?, ?, ?, 'OPEN', ?)
            """,
            (
                leader_wallet,
                token_id,
                amount_usd,
                entry_price,
                signal_id,
            ),
        )
    else:
        old_amount = float(existing["position_usd"])
        old_avg = existing["avg_entry_price"]
        old_avg = float(old_avg) if old_avg is not None else None

        new_amount = old_amount + amount_usd

        if entry_price is not None:
            if old_avg is None or old_amount <= 0:
                new_avg = entry_price
            else:
                new_avg = ((old_amount * old_avg) + (amount_usd * entry_price)) / new_amount
        else:
            new_avg = old_avg

        cur.execute(
            """
            UPDATE copied_positions
            SET position_usd = ?,
                avg_entry_price = ?,
                last_signal_id = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE leader_wallet = ?
              AND token_id = ?
              AND status = 'OPEN'
            """,
            (
                new_amount,
                new_avg,
                signal_id,
                leader_wallet,
                token_id,
            ),
        )

    conn.commit()
    conn.close()


def close_position(
    leader_wallet: str,
    token_id: str,
    signal_id: str,
) -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE copied_positions
        SET position_usd = 0,
            status = 'CLOSED',
            last_signal_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE leader_wallet = ?
          AND token_id = ?
          AND status = 'OPEN'
        """,
        (
            signal_id,
            leader_wallet,
            token_id,
        ),
    )

    conn.commit()
    conn.close()


def list_open_positions(limit: int = 50) -> list[dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT *
        FROM copied_positions
        WHERE status = 'OPEN'
        ORDER BY updated_at DESC
        LIMIT ?
        """,
        (limit,),
    )

    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows


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
