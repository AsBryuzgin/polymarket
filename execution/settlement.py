from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Callable

import requests
from eth_abi import encode
from eth_account import Account
from eth_account.messages import encode_defunct
from eth_utils import keccak
from py_builder_relayer_client.client import RelayClient
from py_builder_relayer_client.models import OperationType, SafeTransaction

from execution.budget_accounting import refresh_active_budgets_from_config
from execution.builder_auth import build_builder_config, load_executor_env
from execution.market_diagnostics import lookup_token_market
from execution.order_router import LIVE_TRADING_ACK, resolve_execution_mode
from execution.position_marking import mark_position
from execution.positions import fetch_exchange_positions
from execution.state_store import (
    close_position_and_log_trade,
    get_leader_registry,
    get_open_position,
    get_processed_settlement,
    list_open_positions,
    list_processed_settlements,
    record_processed_settlement,
)
from execution.trade_notifications import send_trade_notification


DEFAULT_COLLATERAL_TOKEN_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
DEFAULT_CTF_CONTRACT_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
DEFAULT_PROXY_FACTORY_ADDRESS = "0xaB45c5A4B0c941a2F231C04C3f49182e1A254052"
DEFAULT_RELAY_HUB_ADDRESS = "0xD216153c06E857cD7f72665E0aF1d7D82172F494"
DEFAULT_PARENT_COLLECTION_ID = "0x" + ("00" * 32)
DEFAULT_GAS_LIMIT = "10000000"

SUCCESS_STATUSES = {"PAPER_SETTLED", "LIVE_CONFIRMED", "LIVE_EXTERNAL_SETTLED"}
PENDING_STATUSES = {"LIVE_SUBMITTED"}
RETRYABLE_STATUSES = {"LIVE_FAILED", "LIVE_SUBMIT_ERROR", "LIVE_TIMEOUT"}


SnapshotLoader = Callable[[str, str], dict[str, Any]]
MarketLookup = Callable[[str], dict[str, Any] | None]
OpenPositionsLoader = Callable[[int], list[dict[str, Any]]]
ExchangePositionsLoader = Callable[[str], list[dict[str, Any]]]
SleepFn = Callable[[float], None]


def _refresh_active_budgets_after_exit(config: dict[str, Any]) -> dict[str, Any]:
    try:
        return refresh_active_budgets_from_config(config=config)
    except Exception as e:
        return {
            "status": "ERROR",
            "reason": str(e),
        }


@dataclass(frozen=True)
class SettlementPosition:
    leader_wallet: str
    leader_user_name: str | None
    category: str | None
    token_id: str
    position_usd: float
    avg_entry_price: float | None
    qty: float
    settlement_price: float
    payout_usd: float
    opened_at: str | None


@dataclass(frozen=True)
class SettlementCandidate:
    condition_id: str
    question: str | None
    market_slug: str | None
    outcome_count: int
    token_ids: list[str]
    positions: list[SettlementPosition]
    expected_payout_usd: float
    exchange_position_present: bool | None


def _safe_float(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _safe_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return bool(value)


def _strip_0x(value: str) -> str:
    return value[2:] if value.lower().startswith("0x") else value


def _hex_bytes(value: str) -> bytes:
    return bytes.fromhex(_strip_0x(value))


def _encode_call(signature: str, arg_types: list[str], args: list[Any]) -> str:
    selector = keccak(text=signature)[:4]
    calldata = selector + encode(arg_types, args)
    return "0x" + calldata.hex()


def _maybe_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _market_outcome_count(market: dict[str, Any] | None) -> int:
    if not isinstance(market, dict):
        return 2
    for key in ("tokens", "outcomes", "clobTokenIds", "clob_token_ids"):
        items = _maybe_json_list(market.get(key))
        if items:
            return max(len(items), 2)
    return 2


def _index_sets(outcome_count: int) -> list[int]:
    return [1 << idx for idx in range(max(int(outcome_count), 2))]


def _parse_opened_at_to_minutes(opened_at: str | None) -> float | None:
    if not opened_at:
        return None
    try:
        dt = datetime.fromisoformat(opened_at.replace(" ", "T"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return round((datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() / 60.0, 2)


def _settlement_cfg_value(config: dict[str, Any], key: str, default: Any) -> Any:
    return config.get("settlement", {}).get(key, default)


def settlement_enabled(config: dict[str, Any]) -> bool:
    return _safe_bool(_settlement_cfg_value(config, "enabled", True), True)


def build_redeem_positions_calldata(
    *,
    collateral_token_address: str,
    condition_id: str,
    outcome_count: int,
    parent_collection_id: str = DEFAULT_PARENT_COLLECTION_ID,
) -> str:
    return _encode_call(
        "redeemPositions(address,bytes32,bytes32,uint256[])",
        ["address", "bytes32", "bytes32", "uint256[]"],
        [
            collateral_token_address,
            _hex_bytes(parent_collection_id),
            _hex_bytes(condition_id),
            _index_sets(outcome_count),
        ],
    )


def _build_proxy_transaction_data(
    *,
    to: str,
    calldata: str,
    value: int = 0,
    type_code: int = 1,
) -> str:
    return _encode_call(
        "proxy((uint8,address,uint256,bytes)[])",
        ["(uint8,address,uint256,bytes)[]"],
        [[(type_code, to, value, _hex_bytes(calldata))]],
    )


def _create_proxy_struct_hash(
    *,
    from_address: str,
    to: str,
    data: str,
    relayer_fee: str,
    gas_price: str,
    gas_limit: str,
    nonce: str,
    relay_hub_address: str,
    relay_address: str,
) -> bytes:
    return keccak(
        b"rlx:"
        + _hex_bytes(from_address)
        + _hex_bytes(to)
        + _hex_bytes(data)
        + int(relayer_fee).to_bytes(32, "big")
        + int(gas_price).to_bytes(32, "big")
        + int(gas_limit).to_bytes(32, "big")
        + int(nonce).to_bytes(32, "big")
        + _hex_bytes(relay_hub_address)
        + _hex_bytes(relay_address)
    )


def create_proxy_signature(
    *,
    private_key: str,
    from_address: str,
    to: str,
    data: str,
    relayer_fee: str,
    gas_price: str,
    gas_limit: str,
    nonce: str,
    relay_hub_address: str,
    relay_address: str,
) -> str:
    struct_hash = _create_proxy_struct_hash(
        from_address=from_address,
        to=to,
        data=data,
        relayer_fee=relayer_fee,
        gas_price=gas_price,
        gas_limit=gas_limit,
        nonce=nonce,
        relay_hub_address=relay_hub_address,
        relay_address=relay_address,
    )
    signed = Account.from_key(private_key).sign_message(
        encode_defunct(hexstr="0x" + struct_hash.hex())
    )
    return "0x" + signed.signature.hex()


def _builder_headers(method: str, path: str, body: str | None) -> dict[str, str]:
    env = load_executor_env()
    builder_config = build_builder_config(env)
    if builder_config is None:
        return {}
    payload = builder_config.generate_builder_headers(method, path, body)
    return payload.to_dict() if payload is not None else {}


def _get_relayer_json(
    *,
    path: str,
    params: dict[str, Any] | None = None,
    timeout_sec: float = 15.0,
) -> Any:
    env = load_executor_env()
    response = requests.get(
        f"{env.relayer_url.rstrip('/')}{path}",
        params=params,
        timeout=timeout_sec,
    )
    response.raise_for_status()
    return response.json()


def _post_relayer_json(
    *,
    path: str,
    payload: dict[str, Any],
    timeout_sec: float = 20.0,
) -> Any:
    env = load_executor_env()
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    headers = {
        "Content-Type": "application/json",
        **_builder_headers("POST", path, body),
    }
    response = requests.post(
        f"{env.relayer_url.rstrip('/')}{path}",
        data=body.encode("utf-8"),
        headers=headers,
        timeout=timeout_sec,
    )
    response.raise_for_status()
    return response.json()


def _submit_proxy_redeem(
    *,
    config: dict[str, Any],
    call_data: str,
    metadata: str,
) -> dict[str, Any]:
    env = load_executor_env()
    relay_payload = _get_relayer_json(
        path="/relay-payload",
        params={"address": Account.from_key(env.private_key).address, "type": "PROXY"},
    )
    relay_address = str((relay_payload or {}).get("address") or "")
    nonce = str((relay_payload or {}).get("nonce") or "")
    if not relay_address or not nonce:
        raise RuntimeError(f"invalid relay payload: {relay_payload}")

    proxy_factory = str(
        _settlement_cfg_value(config, "proxy_factory_address", DEFAULT_PROXY_FACTORY_ADDRESS)
    )
    relay_hub = str(_settlement_cfg_value(config, "relay_hub_address", DEFAULT_RELAY_HUB_ADDRESS))
    gas_price = str(_settlement_cfg_value(config, "gas_price", "0"))
    gas_limit = str(_settlement_cfg_value(config, "gas_limit", DEFAULT_GAS_LIMIT))
    relayer_fee = "0"

    proxy_data = _build_proxy_transaction_data(
        to=str(_settlement_cfg_value(config, "ctf_contract_address", DEFAULT_CTF_CONTRACT_ADDRESS)),
        calldata=call_data,
    )
    signature = create_proxy_signature(
        private_key=env.private_key,
        from_address=Account.from_key(env.private_key).address,
        to=proxy_factory,
        data=proxy_data,
        relayer_fee=relayer_fee,
        gas_price=gas_price,
        gas_limit=gas_limit,
        nonce=nonce,
        relay_hub_address=relay_hub,
        relay_address=relay_address,
    )

    request_payload = {
        "type": "PROXY",
        "from": Account.from_key(env.private_key).address,
        "to": proxy_factory,
        "proxyWallet": env.funder_address,
        "data": proxy_data,
        "nonce": nonce,
        "signature": signature,
        "signatureParams": {
            "gasPrice": gas_price,
            "gasLimit": gas_limit,
            "relayerFee": relayer_fee,
            "relayHub": relay_hub,
            "relay": relay_address,
        },
        "metadata": metadata,
    }
    raw = _post_relayer_json(path="/submit", payload=request_payload)
    return {
        "transaction_id": raw.get("transactionID"),
        "transaction_hash": raw.get("transactionHash") or raw.get("hash"),
        "raw_response": raw,
    }


def _submit_safe_redeem(
    *,
    config: dict[str, Any],
    call_data: str,
    metadata: str,
) -> dict[str, Any]:
    env = load_executor_env()
    builder_config = build_builder_config(env)
    client = RelayClient(
        env.relayer_url,
        chain_id=env.chain_id,
        private_key=env.private_key,
        builder_config=builder_config,
    )
    response = client.execute(
        [
            SafeTransaction(
                to=str(
                    _settlement_cfg_value(
                        config,
                        "ctf_contract_address",
                        DEFAULT_CTF_CONTRACT_ADDRESS,
                    )
                ),
                operation=OperationType.Call,
                data=call_data,
                value="0",
            )
        ],
        metadata=metadata,
    )
    return {
        "transaction_id": getattr(response, "transaction_id", None),
        "transaction_hash": getattr(response, "transaction_hash", None),
        "raw_response": {
            "transactionID": getattr(response, "transaction_id", None),
            "transactionHash": getattr(response, "transaction_hash", None),
        },
    }


def fetch_relayer_transaction(transaction_id: str) -> dict[str, Any] | None:
    raw = _get_relayer_json(path="/transaction", params={"id": transaction_id})
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                return item
        return None
    return raw if isinstance(raw, dict) else None


def _poll_relayer_transaction(
    *,
    transaction_id: str,
    poll_attempts: int,
    poll_interval_sec: float,
    sleep_fn: SleepFn,
) -> dict[str, Any] | None:
    last_row = None
    for attempt in range(max(int(poll_attempts), 0)):
        if attempt > 0 and poll_interval_sec > 0:
            sleep_fn(poll_interval_sec)
        row = fetch_relayer_transaction(transaction_id)
        if row is None:
            continue
        last_row = row
        state = str(row.get("state") or "")
        if state in {"STATE_MINED", "STATE_CONFIRMED"}:
            return row
        if state == "STATE_FAILED":
            return row
    return last_row


def _live_settlement_allowed(config: dict[str, Any]) -> tuple[bool, str]:
    global_cfg = config.get("global", {})
    if not _safe_bool(global_cfg.get("live_trading_enabled"), False):
        return False, "live trading disabled by config"
    if str(global_cfg.get("live_trading_ack") or "") != LIVE_TRADING_ACK:
        return False, "live trading ack is missing or invalid"
    return True, "ok"


def build_settlement_candidates(
    *,
    config: dict[str, Any],
    snapshot_loader: SnapshotLoader,
    market_lookup: MarketLookup = lookup_token_market,
    open_positions_loader: OpenPositionsLoader = list_open_positions,
    exchange_positions: list[dict[str, Any]] | None = None,
) -> list[SettlementCandidate]:
    registry_rows = {
        str(row.get("wallet") or ""): row
        for row in [get_leader_registry(str(pos.get("leader_wallet") or "")) for pos in open_positions_loader(100000)]
        if row
    }
    grouped: dict[str, dict[str, Any]] = {}
    exchange_token_ids = {
        str(row.get("token_id") or "")
        for row in (exchange_positions or [])
        if row.get("token_id")
    }
    exchange_condition_ids = {
        str(row.get("condition_id") or "")
        for row in (exchange_positions or [])
        if row.get("condition_id")
    }

    for position in open_positions_loader(100000):
        marked = mark_position(
            position,
            snapshot_loader=snapshot_loader,
            snapshot_side="SELL",
        )
        if str(marked.get("snapshot_status") or "").upper() != "SETTLED":
            continue

        token_id = str(marked.get("token_id") or "")
        market = market_lookup(token_id)
        condition_id = str((market or {}).get("condition_id") or "").strip()
        if not condition_id:
            continue

        settlement_price = _safe_float(marked.get("settlement_price"))
        position_usd = _safe_float(marked.get("position_usd")) or 0.0
        avg_entry_price = _safe_float(marked.get("avg_entry_price"))
        qty = _safe_float(marked.get("qty"))
        if (qty is None or qty <= 0) and avg_entry_price and avg_entry_price > 0:
            qty = position_usd / avg_entry_price
        payout_usd = _safe_float(marked.get("mark_value_mid_usd"))
        if settlement_price is None or qty is None or payout_usd is None:
            continue

        leader_wallet = str(marked.get("leader_wallet") or "")
        registry_row = registry_rows.get(leader_wallet) or {}
        position_row = SettlementPosition(
            leader_wallet=leader_wallet,
            leader_user_name=str(registry_row.get("user_name") or "") or None,
            category=str(registry_row.get("category") or "") or None,
            token_id=token_id,
            position_usd=position_usd,
            avg_entry_price=avg_entry_price,
            qty=qty,
            settlement_price=settlement_price,
            payout_usd=payout_usd,
            opened_at=marked.get("opened_at"),
        )

        bucket = grouped.setdefault(
            condition_id,
            {
                "condition_id": condition_id,
                "question": (market or {}).get("question"),
                "market_slug": (market or {}).get("slug"),
                "outcome_count": _market_outcome_count(market),
                "token_ids": set(),
                "positions": [],
                "expected_payout_usd": 0.0,
                "exchange_position_present": None,
            },
        )
        bucket["token_ids"].add(token_id)
        bucket["positions"].append(position_row)
        bucket["expected_payout_usd"] += payout_usd

    rows: list[SettlementCandidate] = []
    for condition_id, bucket in grouped.items():
        token_ids = sorted(bucket["token_ids"])
        exchange_present = None
        if exchange_positions is not None:
            exchange_present = bool(
                condition_id in exchange_condition_ids
                or any(token_id in exchange_token_ids for token_id in token_ids)
            )
        rows.append(
            SettlementCandidate(
                condition_id=condition_id,
                question=bucket["question"],
                market_slug=bucket["market_slug"],
                outcome_count=int(bucket["outcome_count"]),
                token_ids=token_ids,
                positions=list(bucket["positions"]),
                expected_payout_usd=round(float(bucket["expected_payout_usd"]), 8),
                exchange_position_present=exchange_present,
            )
        )
    rows.sort(key=lambda row: row.expected_payout_usd, reverse=True)
    return rows


def _finalize_candidate(
    *,
    config: dict[str, Any],
    candidate: SettlementCandidate,
    mode: str,
    notify: bool = True,
) -> dict[str, Any]:
    closed_rows = 0
    payout_total = 0.0
    signal_id = f"settlement:{candidate.condition_id}"

    for position in candidate.positions:
        current_open = get_open_position(position.leader_wallet, position.token_id)
        if current_open is None:
            continue

        payout_usd = round(float(position.payout_usd), 4)
        holding_minutes = _parse_opened_at_to_minutes(current_open.get("opened_at"))
        reduced = close_position_and_log_trade(
            signal_id=signal_id,
            leader_wallet=position.leader_wallet,
            leader_user_name=position.leader_user_name,
            category=position.category,
            leader_status=(get_leader_registry(position.leader_wallet) or {}).get("leader_status"),
            token_id=position.token_id,
            side="SELL",
            event_type="EXIT",
            price=position.settlement_price,
            gross_value_usd=round(payout_usd, 2),
            exit_price=position.settlement_price,
            holding_minutes=holding_minutes,
            notes=f"{mode.lower()} settlement redeem | condition_id={candidate.condition_id}",
        )
        if reduced is None:
            continue

        payout_total += payout_usd
        position_before_usd = float(reduced["position_before_usd"])
        realized_pnl_usd = reduced.get("realized_pnl_usd")
        realized_pnl_pct = reduced.get("realized_pnl_pct")

        if notify:
            send_trade_notification(
                config=config,
                mode=mode,
                event_type="EXIT",
                leader_wallet=position.leader_wallet,
                leader_user_name=position.leader_user_name,
                category=position.category,
                token_id=position.token_id,
                amount_usd=round(payout_usd, 2),
                price=position.settlement_price,
                position_before_usd=position_before_usd,
                position_after_usd=float(reduced["position_after_usd"]),
                signal_id=signal_id,
                realized_pnl_usd=realized_pnl_usd,
                realized_pnl_pct=realized_pnl_pct,
                holding_minutes=holding_minutes,
                closed_fully=True,
            )
        closed_rows += 1

    budget_rebalance = (
        _refresh_active_budgets_after_exit(config)
        if closed_rows > 0
        else {"status": "SKIPPED", "reason": "no closed rows"}
    )

    return {
        "closed_rows": closed_rows,
        "payout_usd": round(payout_total, 8),
        "budget_rebalance": budget_rebalance,
    }


def _submit_live_candidate(
    *,
    config: dict[str, Any],
    candidate: SettlementCandidate,
    sleep_fn: SleepFn,
) -> dict[str, Any]:
    env = load_executor_env()
    call_data = build_redeem_positions_calldata(
        collateral_token_address=str(
            _settlement_cfg_value(
                config,
                "collateral_token_address",
                DEFAULT_COLLATERAL_TOKEN_ADDRESS,
            )
        ),
        condition_id=candidate.condition_id,
        outcome_count=candidate.outcome_count,
        parent_collection_id=str(
            _settlement_cfg_value(
                config,
                "parent_collection_id",
                DEFAULT_PARENT_COLLECTION_ID,
            )
        ),
    )
    metadata = f"redeem:{candidate.condition_id}"
    if env.signature_type == 1:
        submitted = _submit_proxy_redeem(
            config=config,
            call_data=call_data,
            metadata=metadata,
        )
    elif env.signature_type == 2:
        submitted = _submit_safe_redeem(
            config=config,
            call_data=call_data,
            metadata=metadata,
        )
    else:
        raise RuntimeError(f"unsupported signature_type for settlement: {env.signature_type}")

    transaction_id = str(submitted.get("transaction_id") or "")
    poll_attempts = int(_settlement_cfg_value(config, "poll_attempts", 10))
    poll_interval_sec = float(_settlement_cfg_value(config, "poll_interval_sec", 2.0))
    final_row = (
        _poll_relayer_transaction(
            transaction_id=transaction_id,
            poll_attempts=poll_attempts,
            poll_interval_sec=poll_interval_sec,
            sleep_fn=sleep_fn,
        )
        if transaction_id
        else None
    )
    state = str((final_row or {}).get("state") or "")
    if state in {"STATE_MINED", "STATE_CONFIRMED"}:
        return {
            "status": "LIVE_CONFIRMED",
            "reason": f"redeem transaction reached {state}",
            "transaction_id": submitted.get("transaction_id"),
            "transaction_hash": (final_row or {}).get("transactionHash")
            or submitted.get("transaction_hash"),
            "raw_response": {
                "submit": submitted.get("raw_response"),
                "final": final_row,
            },
        }
    if state == "STATE_FAILED":
        return {
            "status": "LIVE_FAILED",
            "reason": "redeem transaction failed onchain",
            "transaction_id": submitted.get("transaction_id"),
            "transaction_hash": (final_row or {}).get("transactionHash")
            or submitted.get("transaction_hash"),
            "raw_response": {
                "submit": submitted.get("raw_response"),
                "final": final_row,
            },
        }
    return {
        "status": "LIVE_SUBMITTED",
        "reason": "redeem submitted but final mined/confirmed state was not observed yet",
        "transaction_id": submitted.get("transaction_id"),
        "transaction_hash": submitted.get("transaction_hash"),
        "raw_response": {
            "submit": submitted.get("raw_response"),
            "final": final_row,
        },
    }


def _recover_pending_candidate(
    *,
    config: dict[str, Any],
    candidate: SettlementCandidate,
    existing: dict[str, Any],
    sleep_fn: SleepFn,
) -> dict[str, Any]:
    transaction_id = str(existing.get("transaction_id") or "")
    if not transaction_id:
        return {
            "status": "LIVE_TIMEOUT",
            "reason": "pending settlement row is missing transaction_id",
            "transaction_id": None,
            "transaction_hash": existing.get("transaction_hash"),
            "raw_response": None,
        }
    poll_attempts = int(_settlement_cfg_value(config, "poll_attempts", 10))
    poll_interval_sec = float(_settlement_cfg_value(config, "poll_interval_sec", 2.0))
    final_row = _poll_relayer_transaction(
        transaction_id=transaction_id,
        poll_attempts=poll_attempts,
        poll_interval_sec=poll_interval_sec,
        sleep_fn=sleep_fn,
    )
    state = str((final_row or {}).get("state") or "")
    if state in {"STATE_MINED", "STATE_CONFIRMED"}:
        return {
            "status": "LIVE_CONFIRMED",
            "reason": f"redeem transaction reached {state}",
            "transaction_id": transaction_id,
            "transaction_hash": (final_row or {}).get("transactionHash")
            or existing.get("transaction_hash"),
            "raw_response": {"final": final_row},
        }
    if state == "STATE_FAILED":
        return {
            "status": "LIVE_FAILED",
            "reason": "redeem transaction failed onchain",
            "transaction_id": transaction_id,
            "transaction_hash": (final_row or {}).get("transactionHash")
            or existing.get("transaction_hash"),
            "raw_response": {"final": final_row},
        }
    return {
        "status": "LIVE_SUBMITTED",
        "reason": "redeem is still pending in the relayer",
        "transaction_id": transaction_id,
        "transaction_hash": existing.get("transaction_hash"),
        "raw_response": {"final": final_row},
    }


def run_settlement_cycle(
    *,
    config: dict[str, Any],
    snapshot_loader: SnapshotLoader,
    market_lookup: MarketLookup = lookup_token_market,
    open_positions_loader: OpenPositionsLoader = list_open_positions,
    exchange_positions_loader: ExchangePositionsLoader = fetch_exchange_positions,
    sleep_fn: SleepFn = time.sleep,
) -> dict[str, Any]:
    if not settlement_enabled(config):
        return {
            "enabled": False,
            "status": "DISABLED",
            "mode": resolve_execution_mode(config),
            "candidates": 0,
            "processed": 0,
            "closed_rows": 0,
        }

    mode = resolve_execution_mode(config)
    env = load_executor_env()
    exchange_positions = None
    if mode == "LIVE" and _safe_bool(
        _settlement_cfg_value(config, "live_require_exchange_position", True),
        True,
    ):
        try:
            exchange_positions = exchange_positions_loader(env.funder_address)
        except Exception:
            exchange_positions = None

    candidates = build_settlement_candidates(
        config=config,
        snapshot_loader=snapshot_loader,
        market_lookup=market_lookup,
        open_positions_loader=open_positions_loader,
        exchange_positions=exchange_positions,
    )

    processed = 0
    closed_rows = 0
    preview_ready = 0
    live_submitted = 0
    failed = 0
    reasons: list[str] = []

    for candidate in candidates:
        existing = get_processed_settlement(candidate.condition_id)
        if existing and str(existing.get("status") or "") in SUCCESS_STATUSES:
            finalized = _finalize_candidate(
                config=config,
                candidate=candidate,
                mode=str(existing.get("mode") or mode),
                notify=False,
            )
            closed_rows += finalized["closed_rows"]
            processed += 1
            continue

        if mode == "PREVIEW":
            record_processed_settlement(
                candidate.condition_id,
                market_slug=candidate.market_slug,
                question=candidate.question,
                token_ids=candidate.token_ids,
                mode=mode,
                status="PREVIEW_READY",
                reason="resolved position is ready for settlement preview",
                expected_payout_usd=candidate.expected_payout_usd,
                position_count=len(candidate.positions),
                raw_response={"candidate": asdict(candidate)},
            )
            preview_ready += 1
            processed += 1
            continue

        if mode == "PAPER":
            finalized = _finalize_candidate(
                config=config,
                candidate=candidate,
                mode=mode,
            )
            record_processed_settlement(
                candidate.condition_id,
                market_slug=candidate.market_slug,
                question=candidate.question,
                token_ids=candidate.token_ids,
                mode=mode,
                status="PAPER_SETTLED",
                reason="paper settlement applied",
                expected_payout_usd=candidate.expected_payout_usd,
                position_count=len(candidate.positions),
                raw_response={
                    "candidate": asdict(candidate),
                    "finalized": finalized,
                },
            )
            closed_rows += finalized["closed_rows"]
            processed += 1
            continue

        live_ok, live_reason = _live_settlement_allowed(config)
        if not live_ok:
            record_processed_settlement(
                candidate.condition_id,
                market_slug=candidate.market_slug,
                question=candidate.question,
                token_ids=candidate.token_ids,
                mode=mode,
                status="LIVE_BLOCKED",
                reason=live_reason,
                expected_payout_usd=candidate.expected_payout_usd,
                position_count=len(candidate.positions),
                raw_response={"candidate": asdict(candidate)},
            )
            reasons.append(live_reason)
            failed += 1
            processed += 1
            continue

        if candidate.exchange_position_present is False:
            if _safe_bool(
                _settlement_cfg_value(config, "live_finalize_missing_exchange_position", False),
                False,
            ):
                finalized = _finalize_candidate(
                    config=config,
                    candidate=candidate,
                    mode="LIVE_EXTERNAL",
                )
                record_processed_settlement(
                    candidate.condition_id,
                    market_slug=candidate.market_slug,
                    question=candidate.question,
                    token_ids=candidate.token_ids,
                    mode=mode,
                    status="LIVE_EXTERNAL_SETTLED",
                    reason=(
                        "resolved local position had no matching exchange position row; "
                        "finalized as externally settled/auto-claimed"
                    ),
                    expected_payout_usd=candidate.expected_payout_usd,
                    position_count=len(candidate.positions),
                    raw_response={
                        "candidate": asdict(candidate),
                        "finalized": finalized,
                    },
                )
                closed_rows += finalized["closed_rows"]
                processed += 1
                continue

            record_processed_settlement(
                candidate.condition_id,
                market_slug=candidate.market_slug,
                question=candidate.question,
                token_ids=candidate.token_ids,
                mode=mode,
                status="SKIPPED_NO_EXCHANGE_POSITION",
                reason="resolved local position has no matching exchange position row",
                expected_payout_usd=candidate.expected_payout_usd,
                position_count=len(candidate.positions),
                raw_response={"candidate": asdict(candidate)},
            )
            reasons.append("no exchange position row")
            processed += 1
            continue

        if existing and str(existing.get("status") or "") in PENDING_STATUSES:
            execution = _recover_pending_candidate(
                config=config,
                candidate=candidate,
                existing=existing,
                sleep_fn=sleep_fn,
            )
        else:
            try:
                execution = _submit_live_candidate(
                    config=config,
                    candidate=candidate,
                    sleep_fn=sleep_fn,
                )
            except Exception as exc:
                execution = {
                    "status": "LIVE_SUBMIT_ERROR",
                    "reason": str(exc),
                    "transaction_id": None,
                    "transaction_hash": None,
                    "raw_response": None,
                }

        record_processed_settlement(
            candidate.condition_id,
            market_slug=candidate.market_slug,
            question=candidate.question,
            token_ids=candidate.token_ids,
            mode=mode,
            status=str(execution.get("status") or "LIVE_SUBMIT_ERROR"),
            reason=str(execution.get("reason") or ""),
            transaction_id=execution.get("transaction_id"),
            transaction_hash=execution.get("transaction_hash"),
            expected_payout_usd=candidate.expected_payout_usd,
            position_count=len(candidate.positions),
            raw_response=execution.get("raw_response"),
        )

        if execution["status"] == "LIVE_CONFIRMED":
            finalized = _finalize_candidate(
                config=config,
                candidate=candidate,
                mode=mode,
            )
            closed_rows += finalized["closed_rows"]
            processed += 1
            continue

        if execution["status"] == "LIVE_SUBMITTED":
            live_submitted += 1
        else:
            failed += 1
            reasons.append(str(execution.get("reason") or execution["status"]))
        processed += 1

    return {
        "enabled": True,
        "status": "OK",
        "mode": mode,
        "candidates": len(candidates),
        "processed": processed,
        "preview_ready": preview_ready,
        "live_submitted": live_submitted,
        "failed": failed,
        "closed_rows": closed_rows,
        "reasons": reasons[:10],
    }


def build_settlement_report(
    *,
    config: dict[str, Any],
    snapshot_loader: SnapshotLoader,
    market_lookup: MarketLookup = lookup_token_market,
    open_positions_loader: OpenPositionsLoader = list_open_positions,
) -> str:
    candidates = build_settlement_candidates(
        config=config,
        snapshot_loader=snapshot_loader,
        market_lookup=market_lookup,
        open_positions_loader=open_positions_loader,
        exchange_positions=None,
    )
    recent = list_processed_settlements(limit=10)

    lines = ["Сеттлмент / redeem"]
    if not candidates:
        lines.append("активных settlement-кандидатов сейчас нет")
    else:
        expected = sum(row.expected_payout_usd for row in candidates)
        lines.append(
            f"кандидатов: {len(candidates)} | ожидаемый payout: ${expected:.2f}"
        )
        lines.append("")
        lines.append("Текущие кандидаты:")
        for idx, row in enumerate(candidates[:6], start=1):
            lines.append(
                (
                    f"{idx}. {row.question or row.market_slug or row.condition_id[:10]} | "
                    f"positions {len(row.positions)} | payout ${row.expected_payout_usd:.2f}"
                )
            )
            lines.append(
                f"   cond {_strip_0x(row.condition_id)[:10]}... | tokens {len(row.token_ids)}"
            )
            if row.exchange_position_present is False:
                lines.append("   exchange: matching position not found")

    if recent:
        lines.extend(["", "Последние settlement-операции:"])
        for row in recent[:6]:
            lines.append(
                (
                    f"- {(row.get('question') or row.get('market_slug') or row.get('condition_id') or '')[:48]} | "
                    f"{row.get('status')} | payout ${float(row.get('expected_payout_usd') or 0.0):.2f}"
                )
            )
            if row.get("reason"):
                lines.append(f"  {str(row.get('reason'))[:180]}")

    return "\n".join(lines)
