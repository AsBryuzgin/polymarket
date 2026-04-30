from __future__ import annotations

import argparse
import sys
from dataclasses import asdict
from pathlib import Path
from pprint import pprint

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.allowance import evaluate_live_funding_preflight, fetch_collateral_balance_allowance
from execution.builder_auth import health_snapshot, load_executor_config
from execution.order_router import LIVE_TRADING_ACK, execute_market_order, resolve_execution_mode
from execution.polymarket_executor import preview_market_order
from execution.runtime_guard import evaluate_runtime_guard
from execution.state_backup import backup_state_db
from execution.state_store import init_db


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Live-mode smoke test. Default mode never submits an order.",
    )
    parser.add_argument("--token-id", default="", help="Optional token id for a guarded submit test.")
    parser.add_argument("--side", default="BUY", choices=["BUY", "SELL"], help="Submit-test side.")
    parser.add_argument("--amount-usd", type=float, default=0.0, help="Submit-test notional.")
    parser.add_argument("--submit", action="store_true", help="Actually submit a live market order.")
    parser.add_argument(
        "--ack",
        default="",
        help=f"Required with --submit: {LIVE_TRADING_ACK}",
    )
    args = parser.parse_args()

    init_db()
    config = load_executor_config()
    mode = resolve_execution_mode(config)
    runtime_guard = evaluate_runtime_guard(config=config)

    report = {
        "mode": mode,
        "runtime_guard": asdict(runtime_guard),
        "submit_requested": args.submit,
    }

    try:
        report["env_health"] = health_snapshot()
    except Exception as e:
        report["env_health"] = {
            "env_ok": False,
            "api_creds_ok": False,
            "error": str(e),
        }

    try:
        funding = fetch_collateral_balance_allowance(config)
        report["funding"] = {
            "balance_usd": funding.balance_usd,
            "allowance_usd": funding.allowance_usd,
        }
    except Exception as e:
        report["funding"] = {
            "error": str(e),
        }

    if args.amount_usd > 0:
        report["preflight"] = asdict(
            evaluate_live_funding_preflight(
                config=config,
                side=args.side,
                amount_usd=args.amount_usd,
            )
        )

    if not args.submit:
        print("=== LIVE SMOKE TEST (NO ORDER SUBMITTED) ===")
        pprint(report)
        return

    report["backup_before_submit"] = asdict(
        backup_state_db(config=config, label="before_guarded_live_submit")
    )

    if args.ack != LIVE_TRADING_ACK:
        report["submit_blocked"] = "live submit ack is missing or invalid"
        print("=== LIVE SMOKE TEST SUBMIT BLOCKED ===")
        pprint(report)
        raise SystemExit(2)

    if mode != "LIVE":
        report["submit_blocked"] = f"resolved execution mode is {mode}, not LIVE"
        print("=== LIVE SMOKE TEST SUBMIT BLOCKED ===")
        pprint(report)
        raise SystemExit(2)

    if not runtime_guard.allowed:
        report["submit_blocked"] = runtime_guard.reason
        print("=== LIVE SMOKE TEST SUBMIT BLOCKED ===")
        pprint(report)
        raise SystemExit(2)

    if not args.token_id or args.amount_usd <= 0:
        report["submit_blocked"] = "--token-id and positive --amount-usd are required with --submit"
        print("=== LIVE SMOKE TEST SUBMIT BLOCKED ===")
        pprint(report)
        raise SystemExit(2)

    result = execute_market_order(
        config=config,
        token_id=args.token_id,
        amount_usd=args.amount_usd,
        side=args.side,
        preview_fn=preview_market_order,
    )
    report["submit_result"] = asdict(result)
    report["backup_after_submit"] = asdict(
        backup_state_db(config=config, label="after_guarded_live_submit")
    )

    print("=== LIVE SMOKE TEST SUBMIT RESULT ===")
    pprint(report)
    if not result.accepted:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
