from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests
from eth_abi import decode
from eth_utils import keccak

from execution.state_store import get_connection, list_leader_registry


ORDER_MATCHED_SIGNATURE = (
    "OrdersMatched(bytes32,address,uint256,uint256,uint256,uint256)"
)
ORDER_MATCHED_TOPIC = "0x" + keccak(text=ORDER_MATCHED_SIGNATURE).hex()

DEFAULT_EXCHANGE_ADDRESSES = [
    "0xE111180000d2663C0091e4f400237545B87B996B",
    "0xe2222d279d744050d28e00520010520000310F59",
]


@dataclass(frozen=True)
class DecodedMatchedOrder:
    order_hash: str
    leader_wallet: str
    side: str
    token_id: str
    size: float
    price: float | None
    notional_usd: float
    maker_asset_id: int
    taker_asset_id: int
    maker_amount_filled: int
    taker_amount_filled: int


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _parse_utc_timestamp(value: Any) -> int:
    if not value:
        return 0
    text = str(value).replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        try:
            dt = datetime.fromisoformat(text.replace(" ", "T") + "+00:00")
        except ValueError:
            return 0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.astimezone(timezone.utc).timestamp())


def _strip_0x(value: str) -> str:
    return value[2:] if value.lower().startswith("0x") else value


def _hex_to_int(value: str) -> int:
    return int(value, 16)


def _int_to_hex_block(value: int) -> str:
    return hex(max(0, int(value)))


def _topic_address(address: str) -> str:
    return "0x" + _strip_0x(address).lower().rjust(64, "0")


def _address_from_topic(topic: str) -> str:
    raw = _strip_0x(topic)[-40:]
    return "0x" + raw.lower()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def init_onchain_shadow_tables() -> None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS onchain_shadow_fills (
            exchange_address TEXT NOT NULL,
            transaction_hash TEXT NOT NULL,
            log_index INTEGER NOT NULL,
            block_number INTEGER NOT NULL,
            leader_wallet TEXT NOT NULL,
            order_hash TEXT,
            side TEXT,
            token_id TEXT,
            size REAL,
            price REAL,
            notional_usd REAL,
            maker_asset_id TEXT,
            taker_asset_id TEXT,
            maker_amount_filled TEXT,
            taker_amount_filled TEXT,
            observed_at TEXT NOT NULL,
            data_api_seen_at TEXT,
            data_api_trade_timestamp INTEGER,
            raw_log_json TEXT,
            PRIMARY KEY (exchange_address, transaction_hash, log_index)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS onchain_shadow_cursor (
            cursor_key TEXT PRIMARY KEY,
            last_block INTEGER NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()
    conn.close()


def _rpc(rpc_url: str, method: str, params: list[Any], timeout_sec: float = 10.0) -> Any:
    response = requests.post(
        rpc_url,
        json={"jsonrpc": "2.0", "id": int(time.time() * 1000), "method": method, "params": params},
        timeout=timeout_sec,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("error"):
        raise RuntimeError(payload["error"])
    return payload.get("result")


def _current_block(rpc_url: str, timeout_sec: float) -> int:
    return _hex_to_int(str(_rpc(rpc_url, "eth_blockNumber", [], timeout_sec)))


def _decode_orders_matched_log(log: dict[str, Any], *, decimals: int = 6) -> DecodedMatchedOrder | None:
    topics = log.get("topics") or []
    if len(topics) < 3:
        return None
    if str(topics[0]).lower() != ORDER_MATCHED_TOPIC.lower():
        return None

    order_hash = str(topics[1])
    leader_wallet = _address_from_topic(str(topics[2]))
    data = bytes.fromhex(_strip_0x(str(log.get("data") or "0x")))
    try:
        maker_asset_id, taker_asset_id, maker_amount_filled, taker_amount_filled = decode(
            ["uint256", "uint256", "uint256", "uint256"],
            data,
        )
    except Exception:
        return None

    scale = 10 ** int(decimals)
    if int(maker_asset_id) == 0 and int(taker_asset_id) > 0:
        side = "BUY"
        token_id = str(int(taker_asset_id))
        notional_usd = int(maker_amount_filled) / scale
        size = int(taker_amount_filled) / scale
    elif int(taker_asset_id) == 0 and int(maker_asset_id) > 0:
        side = "SELL"
        token_id = str(int(maker_asset_id))
        notional_usd = int(taker_amount_filled) / scale
        size = int(maker_amount_filled) / scale
    else:
        return None

    price = round(notional_usd / size, 8) if size > 0 else None
    return DecodedMatchedOrder(
        order_hash=order_hash,
        leader_wallet=leader_wallet,
        side=side,
        token_id=token_id,
        size=round(size, 8),
        price=price,
        notional_usd=round(notional_usd, 8),
        maker_asset_id=int(maker_asset_id),
        taker_asset_id=int(taker_asset_id),
        maker_amount_filled=int(maker_amount_filled),
        taker_amount_filled=int(taker_amount_filled),
    )


def _cursor_key(chain_id: int, addresses: list[str]) -> str:
    return f"{chain_id}:{','.join(sorted(address.lower() for address in addresses))}"


def _get_cursor(cursor_key: str) -> int | None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT last_block FROM onchain_shadow_cursor WHERE cursor_key = ? LIMIT 1",
        (cursor_key,),
    )
    row = cur.fetchone()
    conn.close()
    return int(row["last_block"]) if row else None


def _set_cursor(cursor_key: str, block_number: int) -> None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO onchain_shadow_cursor (cursor_key, last_block, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(cursor_key) DO UPDATE SET
            last_block = excluded.last_block,
            updated_at = CURRENT_TIMESTAMP
        """,
        (cursor_key, int(block_number)),
    )
    conn.commit()
    conn.close()


def _leader_wallets_from_registry() -> list[str]:
    rows = list_leader_registry(limit=100000)
    return sorted({str(row.get("wallet") or "").lower() for row in rows if row.get("wallet")})


def _insert_shadow_fill(
    *,
    log: dict[str, Any],
    decoded: DecodedMatchedOrder,
    observed_at: str,
) -> bool:
    import json

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO onchain_shadow_fills (
            exchange_address,
            transaction_hash,
            log_index,
            block_number,
            leader_wallet,
            order_hash,
            side,
            token_id,
            size,
            price,
            notional_usd,
            maker_asset_id,
            taker_asset_id,
            maker_amount_filled,
            taker_amount_filled,
            observed_at,
            raw_log_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(log.get("address") or "").lower(),
            str(log.get("transactionHash") or "").lower(),
            _hex_to_int(str(log.get("logIndex") or "0x0")),
            _hex_to_int(str(log.get("blockNumber") or "0x0")),
            decoded.leader_wallet.lower(),
            decoded.order_hash,
            decoded.side,
            decoded.token_id,
            decoded.size,
            decoded.price,
            decoded.notional_usd,
            str(decoded.maker_asset_id),
            str(decoded.taker_asset_id),
            str(decoded.maker_amount_filled),
            str(decoded.taker_amount_filled),
            observed_at,
            json.dumps(log, sort_keys=True),
        ),
    )
    inserted = cur.rowcount > 0
    conn.commit()
    conn.close()
    return inserted


def record_data_api_trade_seen(
    *,
    transaction_hash: str | None,
    leader_wallet: str | None,
    token_id: str | None,
    side: str | None,
    trade_timestamp: int | None,
) -> None:
    if not transaction_hash:
        return
    init_onchain_shadow_tables()
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE onchain_shadow_fills
        SET data_api_seen_at = COALESCE(data_api_seen_at, ?),
            data_api_trade_timestamp = COALESCE(data_api_trade_timestamp, ?)
        WHERE transaction_hash = ?
          AND (? IS NULL OR leader_wallet = lower(?))
          AND (? IS NULL OR token_id = ?)
          AND (? IS NULL OR side = upper(?))
        """,
        (
            _utc_now_iso(),
            trade_timestamp,
            transaction_hash.lower(),
            leader_wallet,
            leader_wallet or "",
            token_id,
            token_id or "",
            side,
            side or "",
        ),
    )
    conn.commit()
    conn.close()


def onchain_signal_id(
    *,
    transaction_hash: str,
    token_id: str,
    side: str,
) -> str:
    return f"onchain:{transaction_hash.lower()}:{str(token_id)}:{side.upper()}"


def list_recent_onchain_shadow_trades(
    *,
    leader_wallet: str,
    limit: int = 50,
    max_age_sec: int = 600,
) -> list[dict[str, Any]]:
    init_onchain_shadow_tables()
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            transaction_hash,
            token_id,
            side,
            MIN(observed_at) AS observed_at,
            SUM(size) AS size,
            SUM(notional_usd) AS notional_usd,
            COUNT(*) AS raw_fills
        FROM onchain_shadow_fills
        WHERE lower(leader_wallet) = lower(?)
          AND datetime(observed_at) >= datetime('now', ?)
          AND transaction_hash IS NOT NULL
          AND transaction_hash != ''
          AND token_id IS NOT NULL
          AND token_id != ''
          AND side IN ('BUY', 'SELL')
        GROUP BY transaction_hash, token_id, side
        ORDER BY observed_at DESC
        LIMIT ?
        """,
        (leader_wallet, f"-{int(max_age_sec)} seconds", int(limit)),
    )
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()

    trades: list[dict[str, Any]] = []
    for row in rows:
        size = _safe_float(row.get("size"))
        notional = _safe_float(row.get("notional_usd"))
        if size <= 0 or notional <= 0:
            continue
        transaction_hash = str(row.get("transaction_hash") or "").lower()
        token_id = str(row.get("token_id") or "")
        side = str(row.get("side") or "").upper()
        trades.append(
            {
                "proxyWallet": leader_wallet,
                "side": side,
                "asset": token_id,
                "conditionId": "",
                "size": size,
                "price": round(notional / size, 8),
                "timestamp": _parse_utc_timestamp(row.get("observed_at")),
                "title": "",
                "slug": "",
                "eventSlug": "",
                "outcome": "",
                "transactionHash": transaction_hash,
                "signalId": onchain_signal_id(
                    transaction_hash=transaction_hash,
                    token_id=token_id,
                    side=side,
                ),
                "source": "onchain_shadow",
                "rawFills": int(row.get("raw_fills") or 0),
            }
        )
    return trades


def poll_onchain_shadow_once(config: dict[str, Any], leader_wallets: list[str] | None = None) -> dict[str, Any]:
    cfg = config.get("onchain_shadow", {})
    if not bool(cfg.get("enabled", False)):
        return {"enabled": False, "status": "DISABLED"}

    init_onchain_shadow_tables()
    rpc_env = str(cfg.get("rpc_url_env") or "POLYGON_RPC_URL")
    rpc_url = os.getenv(rpc_env) or str(
        cfg.get("rpc_url") or "https://polygon-bor-rpc.publicnode.com"
    )
    timeout_sec = float(cfg.get("timeout_sec", 10.0))
    chain_id = int(cfg.get("chain_id", 137))
    decimals = int(cfg.get("decimals", 6))
    confirmation_blocks = int(cfg.get("confirmation_blocks", 2))
    startup_backfill_blocks = int(cfg.get("startup_backfill_blocks", 300))
    max_block_range = max(1, int(cfg.get("max_block_range", 500)))
    exchange_addresses = [
        str(address)
        for address in cfg.get("exchange_addresses", DEFAULT_EXCHANGE_ADDRESSES)
        if str(address).strip()
    ]

    watched_wallets = [wallet.lower() for wallet in (leader_wallets or _leader_wallets_from_registry())]
    watched_wallets = sorted({wallet for wallet in watched_wallets if wallet.startswith("0x")})
    if not watched_wallets:
        return {"enabled": True, "status": "NO_WALLETS", "inserted": 0, "logs": 0}

    current_block = _current_block(rpc_url, timeout_sec)
    to_block = max(0, current_block - confirmation_blocks)
    cursor_key = _cursor_key(chain_id, exchange_addresses)
    cursor = _get_cursor(cursor_key)
    from_block = (to_block - startup_backfill_blocks) if cursor is None else cursor + 1
    from_block = max(0, from_block)
    if from_block > to_block:
        return {
            "enabled": True,
            "status": "UP_TO_DATE",
            "current_block": current_block,
            "cursor": cursor,
            "inserted": 0,
            "logs": 0,
        }

    inserted = 0
    logs_seen = 0
    observed_at = _utc_now_iso()
    wallet_topics = [_topic_address(wallet) for wallet in watched_wallets]

    chunk_from = from_block
    while chunk_from <= to_block:
        chunk_to = min(to_block, chunk_from + max_block_range - 1)
        for exchange_address in exchange_addresses:
            logs = _rpc(
                rpc_url,
                "eth_getLogs",
                [
                    {
                        "fromBlock": _int_to_hex_block(chunk_from),
                        "toBlock": _int_to_hex_block(chunk_to),
                        "address": exchange_address,
                        "topics": [ORDER_MATCHED_TOPIC, None, wallet_topics],
                    }
                ],
                timeout_sec,
            )
            for log in logs or []:
                logs_seen += 1
                decoded = _decode_orders_matched_log(log, decimals=decimals)
                if decoded is None:
                    continue
                if _insert_shadow_fill(log=log, decoded=decoded, observed_at=observed_at):
                    inserted += 1
        chunk_from = chunk_to + 1

    _set_cursor(cursor_key, to_block)
    return {
        "enabled": True,
        "status": "OK",
        "current_block": current_block,
        "from_block": from_block,
        "to_block": to_block,
        "leader_wallets": len(watched_wallets),
        "exchange_addresses": len(exchange_addresses),
        "logs": logs_seen,
        "inserted": inserted,
    }


def onchain_shadow_summary(*, hours: int = 24) -> dict[str, Any]:
    init_onchain_shadow_tables()
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            COUNT(*) AS fills,
            SUM(CASE WHEN data_api_seen_at IS NOT NULL THEN 1 ELSE 0 END) AS matched,
            SUM(CASE WHEN data_api_seen_at IS NULL THEN 1 ELSE 0 END) AS unmatched,
            AVG(
                CASE
                    WHEN data_api_seen_at IS NOT NULL
                    THEN (julianday(data_api_seen_at) - julianday(observed_at)) * 86400.0
                    ELSE NULL
                END
            ) AS avg_data_api_lag_sec
        FROM onchain_shadow_fills
        WHERE datetime(observed_at) >= datetime('now', ?)
        """,
        (f"-{int(hours)} hours",),
    )
    row = dict(cur.fetchone() or {})
    cur.execute(
        """
        SELECT leader_wallet, side, COUNT(*) AS fills
        FROM onchain_shadow_fills
        WHERE datetime(observed_at) >= datetime('now', ?)
        GROUP BY leader_wallet, side
        ORDER BY fills DESC
        LIMIT 20
        """,
        (f"-{int(hours)} hours",),
    )
    by_leader = [dict(item) for item in cur.fetchall()]
    conn.close()
    return {
        "hours": hours,
        "fills": _safe_int(row.get("fills")),
        "matched_data_api": _safe_int(row.get("matched")),
        "unmatched_data_api": _safe_int(row.get("unmatched")),
        "avg_data_api_lag_sec": (
            round(_safe_float(row.get("avg_data_api_lag_sec")), 3)
            if row.get("avg_data_api_lag_sec") is not None
            else None
        ),
        "by_leader": by_leader,
    }
