from __future__ import annotations

import os
import time
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlparse
from typing import Any

import requests
from eth_abi import decode
from eth_utils import keccak

from execution.state_store import _ensure_column, get_connection, list_leader_registry


ORDER_MATCHED_SIGNATURE = (
    # CLOB v2 emits taker-only fills as OrdersMatched(takerOrderHash, takerOrderMaker, ...).
    "OrdersMatched(bytes32,address,uint8,uint256,uint256,uint256)"
)
ORDER_MATCHED_TOPIC = "0x" + keccak(text=ORDER_MATCHED_SIGNATURE).hex()

DEFAULT_EXCHANGE_ADDRESSES = [
    "0xE111180000d2663C0091e4f400237545B87B996B",
    "0xe2222d279d744050d28e00520010520000310F59",
]

DEFAULT_RPC_URLS = [
    "https://polygon.drpc.org",
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon.api.onfinality.io/public",
    "https://1rpc.io/matic",
]

_LAST_GOOD_RPC_URL: str | None = None


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


@dataclass(frozen=True)
class RpcCallResult:
    result: Any
    rpc_url: str
    attempts: int
    errors: tuple[str, ...]


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


def _hex_to_int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(str(value), 16)
    except (TypeError, ValueError):
        return None


def _raw_log_block_timestamp(raw_log_json: Any) -> int | None:
    if not raw_log_json:
        return None
    try:
        raw = json.loads(str(raw_log_json))
    except (TypeError, ValueError):
        return None
    return _hex_to_int_or_none(raw.get("blockTimestamp"))


def _row_trade_timestamp(row: dict[str, Any]) -> int:
    trade_timestamp = _safe_int(row.get("trade_timestamp"))
    if trade_timestamp > 0:
        return trade_timestamp

    data_api_timestamp = _safe_int(row.get("data_api_trade_timestamp"))
    if data_api_timestamp > 0:
        return data_api_timestamp

    block_timestamp = _safe_int(row.get("block_timestamp"))
    if block_timestamp > 0:
        return block_timestamp

    raw_block_timestamp = _raw_log_block_timestamp(row.get("raw_log_json"))
    if raw_block_timestamp:
        return raw_block_timestamp

    return _parse_utc_timestamp(row.get("observed_at"))


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


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        values: list[str] = []
        for item in value:
            values.extend(_as_list(item))
        return values
    text = str(value).strip()
    if not text:
        return []
    normalized = text.replace("\n", ",").replace(";", ",")
    return [item.strip() for item in normalized.split(",") if item.strip()]


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        key = value.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(key)
    return result


def _rpc_url_label(rpc_url: str) -> str:
    parsed = urlparse(rpc_url)
    if parsed.netloc:
        return parsed.netloc
    return str(rpc_url).split("/", 1)[0]


def _configured_rpc_urls(cfg: dict[str, Any]) -> list[str]:
    urls: list[str] = []

    env_names = _as_list(cfg.get("rpc_url_envs"))
    legacy_env_name = str(cfg.get("rpc_url_env") or "POLYGON_RPC_URL").strip()
    if legacy_env_name:
        env_names.insert(0, legacy_env_name)

    for env_name in _dedupe_preserve_order(env_names):
        urls.extend(_as_list(os.getenv(env_name)))

    urls.extend(_as_list(cfg.get("rpc_urls")))
    urls.extend(_as_list(cfg.get("rpc_url")))
    urls.extend(DEFAULT_RPC_URLS)
    return _dedupe_preserve_order(urls)


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
            block_timestamp INTEGER,
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
    _ensure_column(cur, "onchain_shadow_fills", "block_timestamp", "INTEGER")
    cur.execute(
        """
        SELECT exchange_address, transaction_hash, log_index, raw_log_json
        FROM onchain_shadow_fills
        WHERE block_timestamp IS NULL
          AND raw_log_json IS NOT NULL
        """
    )
    timestamp_updates = []
    for row in cur.fetchall():
        block_timestamp = _raw_log_block_timestamp(row["raw_log_json"])
        if block_timestamp:
            timestamp_updates.append(
                (
                    block_timestamp,
                    row["exchange_address"],
                    row["transaction_hash"],
                    row["log_index"],
                )
            )
    if timestamp_updates:
        cur.executemany(
            """
            UPDATE onchain_shadow_fills
            SET block_timestamp = ?
            WHERE exchange_address = ?
              AND transaction_hash = ?
              AND log_index = ?
            """,
            timestamp_updates,
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


def _rpc_once(rpc_url: str, method: str, params: list[Any], timeout_sec: float = 10.0) -> Any:
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


def _prioritize_rpc_urls(rpc_urls: list[str], preferred_url: str | None) -> list[str]:
    if not preferred_url or preferred_url not in rpc_urls:
        return rpc_urls
    return [preferred_url] + [url for url in rpc_urls if url != preferred_url]


def _rpc_with_fallback(
    rpc_urls: list[str],
    method: str,
    params: list[Any],
    *,
    timeout_sec: float,
    retries_per_endpoint: int,
    retry_sleep_sec: float,
    preferred_url: str | None = None,
) -> RpcCallResult:
    global _LAST_GOOD_RPC_URL

    attempts = 0
    errors: list[str] = []
    ordered_urls = _prioritize_rpc_urls(rpc_urls, preferred_url or _LAST_GOOD_RPC_URL)

    for rpc_url in ordered_urls:
        label = _rpc_url_label(rpc_url)
        for attempt in range(max(1, int(retries_per_endpoint))):
            attempts += 1
            try:
                result = _rpc_once(rpc_url, method, params, timeout_sec)
                _LAST_GOOD_RPC_URL = rpc_url
                return RpcCallResult(
                    result=result,
                    rpc_url=rpc_url,
                    attempts=attempts,
                    errors=tuple(errors),
                )
            except Exception as e:
                errors.append(f"{label}:{method}:{type(e).__name__}:{e}")
                if attempt + 1 < max(1, int(retries_per_endpoint)):
                    time.sleep(max(0.0, retry_sleep_sec))

    raise RuntimeError(f"all RPC endpoints failed for {method}: {' | '.join(errors[-6:])}")


def _current_block(
    rpc_urls: list[str],
    *,
    timeout_sec: float,
    retries_per_endpoint: int,
    retry_sleep_sec: float,
) -> tuple[int, RpcCallResult]:
    call = _rpc_with_fallback(
        rpc_urls,
        "eth_blockNumber",
        [],
        timeout_sec=timeout_sec,
        retries_per_endpoint=retries_per_endpoint,
        retry_sleep_sec=retry_sleep_sec,
    )
    return _hex_to_int(str(call.result)), call


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
        side_raw, token_id_raw, maker_amount_filled, taker_amount_filled = decode(
            ["uint8", "uint256", "uint256", "uint256"],
            data,
        )
    except Exception:
        return None

    scale = 10 ** int(decimals)
    side_int = int(side_raw)
    token_id_int = int(token_id_raw)
    if side_int == 0 and token_id_int > 0:
        side = "BUY"
        token_id = str(token_id_int)
        notional_usd = int(maker_amount_filled) / scale
        size = int(taker_amount_filled) / scale
        maker_asset_id = 0
        taker_asset_id = token_id_int
    elif side_int == 1 and token_id_int > 0:
        side = "SELL"
        token_id = str(token_id_int)
        notional_usd = int(taker_amount_filled) / scale
        size = int(maker_amount_filled) / scale
        maker_asset_id = token_id_int
        taker_asset_id = 0
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
            block_timestamp,
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
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(log.get("address") or "").lower(),
            str(log.get("transactionHash") or "").lower(),
            _hex_to_int(str(log.get("logIndex") or "0x0")),
            _hex_to_int(str(log.get("blockNumber") or "0x0")),
            _hex_to_int_or_none(log.get("blockTimestamp")),
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
            MAX(
                COALESCE(
                    block_timestamp,
                    data_api_trade_timestamp,
                    CAST(strftime('%s', observed_at) AS INTEGER)
                )
            ) AS trade_timestamp,
            MAX(block_timestamp) AS block_timestamp,
            MIN(observed_at) AS observed_at,
            MIN(data_api_seen_at) AS data_api_seen_at,
            MAX(data_api_trade_timestamp) AS data_api_trade_timestamp,
            MAX(raw_log_json) AS raw_log_json,
            SUM(size) AS size,
            SUM(notional_usd) AS notional_usd,
            COUNT(*) AS raw_fills
        FROM onchain_shadow_fills
        WHERE lower(leader_wallet) = lower(?)
          AND transaction_hash IS NOT NULL
          AND transaction_hash != ''
          AND token_id IS NOT NULL
          AND token_id != ''
          AND side IN ('BUY', 'SELL')
        GROUP BY transaction_hash, token_id, side
        HAVING trade_timestamp >= CAST(strftime('%s', 'now', ?) AS INTEGER)
        ORDER BY trade_timestamp DESC
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
                "timestamp": _row_trade_timestamp(row),
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


def _record_rpc_success(successes: dict[str, int], rpc_url: str) -> None:
    label = _rpc_url_label(rpc_url)
    successes[label] = successes.get(label, 0) + 1


def _fetch_logs_for_range(
    *,
    rpc_urls: list[str],
    exchange_addresses: list[str],
    wallet_topics: list[str],
    from_block: int,
    to_block: int,
    timeout_sec: float,
    retries_per_endpoint: int,
    retry_sleep_sec: float,
    max_block_range: int,
) -> tuple[list[dict[str, Any]], dict[str, int], list[str], int]:
    logs_out: list[dict[str, Any]] = []
    rpc_successes: dict[str, int] = {}
    rpc_errors: list[str] = []
    rpc_attempts = 0

    chunk_from = from_block
    while chunk_from <= to_block:
        chunk_to = min(to_block, chunk_from + max_block_range - 1)
        for exchange_address in exchange_addresses:
            call = _rpc_with_fallback(
                rpc_urls,
                "eth_getLogs",
                [
                    {
                        "fromBlock": _int_to_hex_block(chunk_from),
                        "toBlock": _int_to_hex_block(chunk_to),
                        "address": exchange_address,
                        "topics": [ORDER_MATCHED_TOPIC, None, wallet_topics],
                    }
                ],
                timeout_sec=timeout_sec,
                retries_per_endpoint=retries_per_endpoint,
                retry_sleep_sec=retry_sleep_sec,
            )
            rpc_attempts += call.attempts
            rpc_errors.extend(call.errors)
            _record_rpc_success(rpc_successes, call.rpc_url)
            logs_out.extend(call.result or [])
        chunk_from = chunk_to + 1

    return logs_out, rpc_successes, rpc_errors, rpc_attempts


def poll_onchain_shadow_once(config: dict[str, Any], leader_wallets: list[str] | None = None) -> dict[str, Any]:
    cfg = config.get("onchain_shadow", {})
    if not bool(cfg.get("enabled", False)):
        return {"enabled": False, "status": "DISABLED"}

    init_onchain_shadow_tables()
    rpc_urls = _configured_rpc_urls(cfg)
    timeout_sec = float(cfg.get("timeout_sec", 10.0))
    retries_per_endpoint = max(1, int(cfg.get("retries_per_endpoint", 1)))
    retry_sleep_sec = float(cfg.get("retry_sleep_sec", 0.15))
    chain_id = int(cfg.get("chain_id", 137))
    decimals = int(cfg.get("decimals", 6))
    confirmation_blocks = int(cfg.get("confirmation_blocks", 2))
    adaptive_backoff_blocks = max(0, int(cfg.get("adaptive_backoff_blocks", 12)))
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

    current_block, current_block_call = _current_block(
        rpc_urls,
        timeout_sec=timeout_sec,
        retries_per_endpoint=retries_per_endpoint,
        retry_sleep_sec=retry_sleep_sec,
    )
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
            "rpc_endpoints": len(rpc_urls),
            "rpc_endpoint": _rpc_url_label(current_block_call.rpc_url),
            "rpc_failover_count": len(current_block_call.errors),
        }

    inserted = 0
    logs_seen = 0
    observed_at = _utc_now_iso()
    wallet_topics = [_topic_address(wallet) for wallet in watched_wallets]

    rpc_errors = list(current_block_call.errors)
    rpc_successes: dict[str, int] = {}
    _record_rpc_success(rpc_successes, current_block_call.rpc_url)
    rpc_attempts = current_block_call.attempts
    processed_to_block = to_block
    status = "OK"

    try:
        logs, log_rpc_successes, log_rpc_errors, log_rpc_attempts = _fetch_logs_for_range(
            rpc_urls=rpc_urls,
            exchange_addresses=exchange_addresses,
            wallet_topics=wallet_topics,
            from_block=from_block,
            to_block=to_block,
            timeout_sec=timeout_sec,
            retries_per_endpoint=retries_per_endpoint,
            retry_sleep_sec=retry_sleep_sec,
            max_block_range=max_block_range,
        )
    except Exception as first_error:
        retry_to_block = to_block - adaptive_backoff_blocks
        if adaptive_backoff_blocks <= 0 or retry_to_block < from_block:
            raise
        processed_to_block = retry_to_block
        status = "OK_BACKOFF"
        rpc_errors.append(str(first_error))
        logs, log_rpc_successes, log_rpc_errors, log_rpc_attempts = _fetch_logs_for_range(
            rpc_urls=rpc_urls,
            exchange_addresses=exchange_addresses,
            wallet_topics=wallet_topics,
            from_block=from_block,
            to_block=processed_to_block,
            timeout_sec=timeout_sec,
            retries_per_endpoint=retries_per_endpoint,
            retry_sleep_sec=retry_sleep_sec,
            max_block_range=max(1, min(max_block_range, 50)),
        )

    rpc_attempts += log_rpc_attempts
    rpc_errors.extend(log_rpc_errors)
    for label, count in log_rpc_successes.items():
        rpc_successes[label] = rpc_successes.get(label, 0) + count

    for log in logs:
        logs_seen += 1
        decoded = _decode_orders_matched_log(log, decimals=decimals)
        if decoded is None:
            continue
        if _insert_shadow_fill(log=log, decoded=decoded, observed_at=observed_at):
            inserted += 1

    _set_cursor(cursor_key, processed_to_block)
    return {
        "enabled": True,
        "status": status,
        "current_block": current_block,
        "from_block": from_block,
        "to_block": processed_to_block,
        "target_to_block": to_block,
        "leader_wallets": len(watched_wallets),
        "exchange_addresses": len(exchange_addresses),
        "logs": logs_seen,
        "inserted": inserted,
        "rpc_endpoints": len(rpc_urls),
        "rpc_successes": rpc_successes,
        "rpc_attempts": rpc_attempts,
        "rpc_failover_count": len(rpc_errors),
        "rpc_errors_sample": rpc_errors[:5],
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
                    WHEN data_api_seen_at IS NOT NULL AND block_timestamp IS NOT NULL
                    THEN (julianday(data_api_seen_at) - julianday(datetime(block_timestamp, 'unixepoch'))) * 86400.0
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
