from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from pprint import pprint
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.builder_auth import load_executor_config
from execution.market_cache import (
    init_market_cache_table,
    list_market_cache_token_ids,
    upsert_market_cache_from_ws,
)
from execution.state_store import init_db


DEFAULT_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


def _load_websocket_client():
    try:
        import websocket  # type: ignore
    except ImportError as e:
        raise RuntimeError(
            "websocket-client is required for market_ws_cache_loop. "
            "Install requirements.txt on the server."
        ) from e
    return websocket


def _messages_from_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def _connect_and_stream(
    *,
    ws_url: str,
    token_ids: list[str],
    refresh_sec: float,
    ping_sec: float,
    timeout_sec: float,
) -> dict[str, Any]:
    websocket = _load_websocket_client()
    started = time.monotonic()
    next_ping = started + ping_sec
    messages = 0
    updates = 0

    ws = websocket.create_connection(ws_url, timeout=timeout_sec)
    try:
        ws.send(json.dumps({"assets_ids": token_ids, "type": "market"}))

        while time.monotonic() - started < refresh_sec:
            now = time.monotonic()
            if now >= next_ping:
                try:
                    ws.send("PING")
                except Exception:
                    break
                next_ping = now + ping_sec

            try:
                raw = ws.recv()
            except Exception:
                continue

            if not raw or raw in {"PONG", "PING"}:
                continue
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue

            for message in _messages_from_payload(payload):
                messages += 1
                if upsert_market_cache_from_ws(message):
                    updates += 1
    finally:
        ws.close()

    return {
        "tokens": len(token_ids),
        "messages": messages,
        "updates": updates,
        "elapsed_sec": round(time.monotonic() - started, 3),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Maintain a CLOB market WebSocket snapshot cache.")
    parser.add_argument("--refresh-sec", type=float, default=None)
    parser.add_argument("--recent-token-minutes", type=int, default=None)
    parser.add_argument("--max-tokens", type=int, default=None)
    parser.add_argument("--sleep-empty-sec", type=float, default=10.0)
    parser.add_argument("--max-cycles", type=int, default=0)
    args = parser.parse_args()

    config = load_executor_config()
    cfg = config.get("market_cache", {})
    enabled = bool(cfg.get("enabled", True))
    if not enabled:
        raise SystemExit("market_cache.enabled is false")

    ws_url = str(cfg.get("ws_url") or DEFAULT_WS_URL)
    refresh_sec = float(args.refresh_sec or cfg.get("refresh_sec", 60.0))
    ping_sec = float(cfg.get("ping_sec", 10.0))
    timeout_sec = float(cfg.get("timeout_sec", 5.0))
    recent_token_minutes = int(args.recent_token_minutes or cfg.get("recent_token_minutes", 360))
    max_tokens = int(args.max_tokens or cfg.get("max_tokens", 250))

    init_db()
    init_market_cache_table()

    print("=== MARKET WS CACHE LOOP ===")
    pprint(
        {
            "ws_url": ws_url,
            "refresh_sec": refresh_sec,
            "recent_token_minutes": recent_token_minutes,
            "max_tokens": max_tokens,
        }
    )

    cycle = 0
    while True:
        cycle += 1
        token_ids = list_market_cache_token_ids(
            recent_minutes=recent_token_minutes,
            max_tokens=max_tokens,
        )
        if not token_ids:
            print(f"cycle {cycle}: no tokens to subscribe; sleeping {args.sleep_empty_sec}s")
            time.sleep(args.sleep_empty_sec)
        else:
            try:
                summary = _connect_and_stream(
                    ws_url=ws_url,
                    token_ids=token_ids,
                    refresh_sec=refresh_sec,
                    ping_sec=ping_sec,
                    timeout_sec=timeout_sec,
                )
            except Exception as e:
                summary = {"tokens": len(token_ids), "error": str(e)}
                time.sleep(min(args.sleep_empty_sec, 10.0))
            print(f"cycle {cycle}:")
            pprint(summary)

        if args.max_cycles and cycle >= args.max_cycles:
            return


if __name__ == "__main__":
    main()

