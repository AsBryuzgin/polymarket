from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping
import tomllib


DEFAULT_DB_PATH = Path("data/executor_state.db")
DB_PATH_ENV_VAR = "POLY_EXECUTOR_DB_PATH"
CONFIG_PATH_ENV_VAR = "POLY_EXECUTOR_CONFIG_PATH"


def resolve_state_db_path(
    *,
    config_path: str | Path = "config/executor.toml",
    env: Mapping[str, str] | None = None,
) -> Path:
    if env is None:
        env = os.environ
    env_value = env.get(DB_PATH_ENV_VAR)
    if env_value:
        return Path(env_value)

    if str(config_path) == "config/executor.toml" and env.get(CONFIG_PATH_ENV_VAR):
        config_path = str(env[CONFIG_PATH_ENV_VAR])

    p = Path(config_path)
    if p.exists():
        try:
            with p.open("rb") as f:
                config = tomllib.load(f)
            configured = config.get("state", {}).get("db_path")
            if configured:
                return Path(str(configured))
        except Exception:
            return DEFAULT_DB_PATH

    return DEFAULT_DB_PATH


DB_PATH = resolve_state_db_path()
EPS = 1e-12


def _table_columns(cur: sqlite3.Cursor, table_name: str) -> set[str]:
    cur.execute(f"PRAGMA table_info({table_name})")
    return {str(row[1]) for row in cur.fetchall()}


def _ensure_column(cur: sqlite3.Cursor, table_name: str, column_name: str, column_sql: str) -> None:
    if column_name not in _table_columns(cur, table_name):
        cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")


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
        CREATE TABLE IF NOT EXISTS order_attempts (
            attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id TEXT NOT NULL,
            leader_wallet TEXT NOT NULL,
            token_id TEXT NOT NULL,
            side TEXT NOT NULL,
            amount_usd REAL NOT NULL,
            mode TEXT NOT NULL,
            status TEXT NOT NULL,
            order_id TEXT,
            fill_amount_usd REAL,
            reason TEXT,
            raw_response_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    _ensure_column(cur, "order_attempts", "order_id", "TEXT")
    _ensure_column(cur, "order_attempts", "fill_amount_usd", "REAL")

    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_order_attempts_signal_id
        ON order_attempts(signal_id)
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

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS leader_registry (
            wallet TEXT PRIMARY KEY,
            category TEXT NOT NULL,
            user_name TEXT,
            leader_status TEXT NOT NULL,
            target_weight REAL,
            target_budget_usd REAL,
            grace_until TEXT,
            source_tag TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS trade_history (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            signal_id TEXT,
            leader_wallet TEXT NOT NULL,
            leader_user_name TEXT,
            category TEXT,
            leader_status TEXT,
            token_id TEXT NOT NULL,
            side TEXT NOT NULL,
            event_type TEXT NOT NULL,
            amount_usd REAL,
            price REAL,
            gross_value_usd REAL,
            position_before_usd REAL,
            position_after_usd REAL,
            entry_avg_price REAL,
            exit_price REAL,
            realized_pnl_usd REAL,
            realized_pnl_pct REAL,
            holding_minutes REAL,
            notes TEXT
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


def claim_signal(
    signal_id: str,
    leader_wallet: str,
    token_id: str,
    side: str,
    leader_budget_usd: float,
) -> bool:
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO processed_signals (
                signal_id,
                leader_wallet,
                token_id,
                side,
                leader_budget_usd,
                suggested_amount_usd,
                status,
                reason
            ) VALUES (?, ?, ?, ?, ?, NULL, 'PROCESSING', 'signal claimed')
            """,
            (
                signal_id,
                leader_wallet,
                token_id,
                side,
                leader_budget_usd,
            ),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


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
        INSERT INTO processed_signals (
            signal_id,
            leader_wallet,
            token_id,
            side,
            leader_budget_usd,
            suggested_amount_usd,
            status,
            reason
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(signal_id) DO UPDATE SET
            leader_wallet = excluded.leader_wallet,
            token_id = excluded.token_id,
            side = excluded.side,
            leader_budget_usd = excluded.leader_budget_usd,
            suggested_amount_usd = excluded.suggested_amount_usd,
            status = excluded.status,
            reason = excluded.reason
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


def create_order_attempt(
    signal_id: str,
    leader_wallet: str,
    token_id: str,
    side: str,
    amount_usd: float,
    mode: str,
    status: str,
    reason: str | None = None,
) -> int:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO order_attempts (
            signal_id,
            leader_wallet,
            token_id,
            side,
            amount_usd,
            mode,
            status,
            reason
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            signal_id,
            leader_wallet,
            token_id,
            side,
            amount_usd,
            mode,
            status,
            reason,
        ),
    )
    attempt_id = int(cur.lastrowid)

    conn.commit()
    conn.close()

    return attempt_id


def update_order_attempt(
    attempt_id: int,
    status: str,
    reason: str | None = None,
    raw_response: dict[str, Any] | None = None,
    order_id: str | None = None,
    fill_amount_usd: float | None = None,
) -> None:
    raw_response_json = None
    if raw_response is not None:
        raw_response_json = json.dumps(raw_response, ensure_ascii=False, default=str)

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE order_attempts
        SET status = ?,
            reason = ?,
            order_id = ?,
            fill_amount_usd = ?,
            raw_response_json = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE attempt_id = ?
        """,
        (
            status,
            reason,
            order_id,
            fill_amount_usd,
            raw_response_json,
            attempt_id,
        ),
    )

    conn.commit()
    conn.close()


def list_order_attempts(signal_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor()

    if signal_id is None:
        cur.execute(
            """
            SELECT *
            FROM order_attempts
            ORDER BY attempt_id DESC
            LIMIT ?
            """,
            (limit,),
        )
    else:
        cur.execute(
            """
            SELECT *
            FROM order_attempts
            WHERE signal_id = ?
            ORDER BY attempt_id DESC
            LIMIT ?
            """,
            (signal_id, limit),
        )

    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows


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


def get_position_any_status(leader_wallet: str, token_id: str) -> dict[str, Any] | None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT *
        FROM copied_positions
        WHERE leader_wallet = ?
          AND token_id = ?
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
) -> dict[str, Any]:
    existing_open = get_open_position(leader_wallet, token_id)
    existing_any = get_position_any_status(leader_wallet, token_id)

    conn = get_connection()
    cur = conn.cursor()

    old_amount = 0.0
    old_avg = None

    if existing_open is None and existing_any is None:
        cur.execute(
            """
            INSERT INTO copied_positions (
                leader_wallet,
                token_id,
                position_usd,
                avg_entry_price,
                status,
                last_signal_id,
                opened_at,
                updated_at
            ) VALUES (?, ?, ?, ?, 'OPEN', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (
                leader_wallet,
                token_id,
                amount_usd,
                entry_price,
                signal_id,
            ),
        )
        new_amount = amount_usd
        new_avg = entry_price

    elif existing_open is None and existing_any is not None:
        cur.execute(
            """
            UPDATE copied_positions
            SET position_usd = ?,
                avg_entry_price = ?,
                status = 'OPEN',
                last_signal_id = ?,
                opened_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE leader_wallet = ?
              AND token_id = ?
            """,
            (
                amount_usd,
                entry_price,
                signal_id,
                leader_wallet,
                token_id,
            ),
        )
        new_amount = amount_usd
        new_avg = entry_price

    else:
        old_amount = float(existing_open["position_usd"])
        old_avg = existing_open["avg_entry_price"]
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

    return {
        "position_before_usd": old_amount,
        "position_after_usd": new_amount,
        "entry_avg_price_before": old_avg,
        "entry_avg_price_after": new_avg,
    }


def reduce_or_close_position(
    leader_wallet: str,
    token_id: str,
    signal_id: str,
    amount_usd: float,
) -> dict[str, Any] | None:
    existing = get_open_position(leader_wallet, token_id)
    if existing is None:
        return None

    old_position = float(existing["position_usd"])
    sell_amount = min(max(float(amount_usd), 0.0), old_position)
    new_position = old_position - sell_amount

    if new_position <= EPS:
        new_position = 0.0
        new_status = "CLOSED"
    else:
        new_status = "OPEN"

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE copied_positions
        SET position_usd = ?,
            status = ?,
            last_signal_id = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE leader_wallet = ?
          AND token_id = ?
          AND status = 'OPEN'
        """,
        (
            new_position,
            new_status,
            signal_id,
            leader_wallet,
            token_id,
        ),
    )

    conn.commit()
    conn.close()

    return {
        "position_before_usd": old_position,
        "sell_amount_usd": sell_amount,
        "position_after_usd": new_position,
        "entry_avg_price": float(existing["avg_entry_price"]) if existing["avg_entry_price"] is not None else None,
        "opened_at": existing["opened_at"],
        "closed_fully": new_status == "CLOSED",
    }


def close_position(
    leader_wallet: str,
    token_id: str,
    signal_id: str,
) -> dict[str, Any] | None:
    existing = get_open_position(leader_wallet, token_id)
    if existing is None:
        return None

    return reduce_or_close_position(
        leader_wallet=leader_wallet,
        token_id=token_id,
        signal_id=signal_id,
        amount_usd=float(existing["position_usd"]),
    )


def log_trade_event(
    signal_id: str | None,
    leader_wallet: str,
    leader_user_name: str | None,
    category: str | None,
    leader_status: str | None,
    token_id: str,
    side: str,
    event_type: str,
    amount_usd: float | None,
    price: float | None,
    gross_value_usd: float | None,
    position_before_usd: float | None,
    position_after_usd: float | None,
    entry_avg_price: float | None,
    exit_price: float | None,
    realized_pnl_usd: float | None,
    realized_pnl_pct: float | None,
    holding_minutes: float | None,
    notes: str | None,
) -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO trade_history (
            signal_id,
            leader_wallet,
            leader_user_name,
            category,
            leader_status,
            token_id,
            side,
            event_type,
            amount_usd,
            price,
            gross_value_usd,
            position_before_usd,
            position_after_usd,
            entry_avg_price,
            exit_price,
            realized_pnl_usd,
            realized_pnl_pct,
            holding_minutes,
            notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            signal_id,
            leader_wallet,
            leader_user_name,
            category,
            leader_status,
            token_id,
            side,
            event_type,
            amount_usd,
            price,
            gross_value_usd,
            position_before_usd,
            position_after_usd,
            entry_avg_price,
            exit_price,
            realized_pnl_usd,
            realized_pnl_pct,
            holding_minutes,
            notes,
        ),
    )

    conn.commit()
    conn.close()


def upsert_leader_registry_row(
    wallet: str,
    category: str,
    user_name: str,
    leader_status: str,
    target_weight: float,
    target_budget_usd: float,
    grace_until: str | None,
    source_tag: str,
) -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO leader_registry (
            wallet,
            category,
            user_name,
            leader_status,
            target_weight,
            target_budget_usd,
            grace_until,
            source_tag
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(wallet) DO UPDATE SET
            category = excluded.category,
            user_name = excluded.user_name,
            leader_status = excluded.leader_status,
            target_weight = excluded.target_weight,
            target_budget_usd = excluded.target_budget_usd,
            grace_until = excluded.grace_until,
            source_tag = excluded.source_tag,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            wallet,
            category,
            user_name,
            leader_status,
            target_weight,
            target_budget_usd,
            grace_until,
            source_tag,
        ),
    )

    conn.commit()
    conn.close()


def get_leader_registry(wallet: str) -> dict[str, Any] | None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        "SELECT * FROM leader_registry WHERE wallet = ? LIMIT 1",
        (wallet,),
    )
    row = cur.fetchone()
    conn.close()

    return dict(row) if row else None


def list_leader_registry(limit: int = 100) -> list[dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT *
        FROM leader_registry
        ORDER BY category ASC, wallet ASC
        LIMIT ?
        """,
        (limit,),
    )

    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows


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


def list_processed_signals(limit: int = 100000) -> list[dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT *
        FROM processed_signals
        ORDER BY created_at ASC
        LIMIT ?
        """,
        (limit,),
    )

    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows


def list_trade_history(limit: int = 200) -> list[dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT *
        FROM trade_history
        ORDER BY event_id DESC
        LIMIT ?
        """,
        (limit,),
    )

    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows


def sum_realized_pnl_since(since: datetime) -> float:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT COALESCE(SUM(realized_pnl_usd), 0.0) AS realized_pnl
        FROM trade_history
        WHERE realized_pnl_usd IS NOT NULL
          AND event_time >= ?
        """,
        (since.strftime("%Y-%m-%d %H:%M:%S"),),
    )
    row = cur.fetchone()
    conn.close()

    return float(row["realized_pnl"] or 0.0)
