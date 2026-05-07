from __future__ import annotations

import asyncio
import csv
import sys
import time
from pathlib import Path
from pprint import pprint

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.allocation_runtime import resolve_leader_budget_usd, resolve_total_capital_usd
from collectors.wallet_profiles import WalletProfilesClient
from execution.builder_auth import load_executor_config
from execution.copy_worker import process_signal
from execution.leader_signal_source import latest_fresh_copyable_signal_from_wallet
from execution.polling import remaining_cycle_sleep_sec
from execution.state_backup import backup_state_db
from execution.state_store import init_db


INPUT_FILE = Path("data/shortlists/live_portfolio_allocation.csv")


def load_allocation(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Missing allocation file: {path}")

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    for row in rows:
        row["weight"] = float(row["weight"])
        row["final_wss"] = float(row["final_wss"])
        row["leaderboard_pnl"] = float(row["leaderboard_pnl"])
        row["leaderboard_volume"] = float(row["leaderboard_volume"])

    return rows


def fetch_latest_trade_sync(wallet: str) -> dict | None:
    client = WalletProfilesClient()
    trades = client.get_trades(user=wallet, limit=3, offset=0, taker_only=True)

    if not trades:
        return None

    normalized = []
    for trade in trades:
        try:
            normalized.append(
                {
                    "transaction_hash": str(trade.get("transactionHash") or ""),
                    "side": str(trade.get("side") or "").upper(),
                    "asset": str(trade.get("asset") or ""),
                    "price": float(trade.get("price") or 0),
                    "timestamp": int(trade.get("timestamp") or 0),
                }
            )
        except Exception:
            continue

    normalized = [x for x in normalized if x["transaction_hash"] and x["timestamp"] > 0]
    if not normalized:
        return None

    latest = max(normalized, key=lambda x: x["timestamp"])
    return latest


async def fetch_latest_trade(wallet: str) -> dict | None:
    return await asyncio.to_thread(fetch_latest_trade_sync, wallet)


async def bootstrap_last_seen(rows: list[dict]) -> tuple[dict[str, str], dict[str, float]]:
    tasks = [fetch_latest_trade(row["wallet"]) for row in rows]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    last_seen_by_wallet: dict[str, str] = {}
    first_seen_by_tx: dict[str, float] = {}

    print("=== Bootstrap latest tx hashes ===")
    now_ts = time.time()

    for row, result in zip(rows, results):
        wallet = row["wallet"]
        user_name = row["user_name"]

        if isinstance(result, Exception):
            print(f"{user_name}: bootstrap_error={result}")
            continue

        if not result or not result.get("transaction_hash"):
            print(f"{user_name}: no latest trade")
            continue

        tx = result["transaction_hash"]
        last_seen_by_wallet[wallet] = tx
        first_seen_by_tx[tx] = now_ts

        age_sec = int(now_ts) - int(result["timestamp"])
        print(
            f"{user_name}: side={result['side']} | age={age_sec}s | "
            f"tx={tx[:12]}..."
        )

    print()
    return last_seen_by_wallet, first_seen_by_tx


async def main_async() -> None:
    init_db()
    config = load_executor_config()
    poll_interval_sec = float(config.get("global", {}).get("poll_interval_sec", 2))
    preferred_signal_age_sec = int(config.get("signal_freshness", {}).get("preferred_signal_age_sec", 30))
    max_signal_age_sec = int(config.get("signal_freshness", {}).get("max_signal_age_sec", 90))
    total_capital_usd = resolve_total_capital_usd(executor_config=config)

    rows = load_allocation(INPUT_FILE)
    last_seen_by_wallet, first_seen_by_tx = await bootstrap_last_seen(rows)

    print("=== Multi-Leader Live Poll (async prescan) ===")
    print(f"leaders={len(rows)} | poll_interval_sec={poll_interval_sec}")
    pprint({"startup_backup": backup_state_db(config=config, label="startup")})
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            cycle_started_monotonic = time.monotonic()
            cycle_started = time.strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n--- cycle started at {cycle_started} ---")

            tasks = [fetch_latest_trade(row["wallet"]) for row in rows]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for idx, (row, latest) in enumerate(zip(rows, results), start=1):
                wallet = row["wallet"]
                user_name = row["user_name"]
                category = row["category"]
                leader_budget_usd = resolve_leader_budget_usd(
                    row,
                    total_capital_usd=total_capital_usd,
                )

                print(f"[{idx}/{len(rows)}] {user_name} | {category} | budget=${leader_budget_usd} | {wallet}")

                if isinstance(latest, Exception):
                    print(f"  prescan_error: {latest}")
                    continue

                if latest is None:
                    print("  no latest trade")
                    continue

                latest_tx = latest.get("transaction_hash")
                latest_side = latest.get("side")
                latest_ts = int(latest.get("timestamp") or 0)
                latest_age = int(time.time()) - latest_ts if latest_ts else None

                if not latest_tx:
                    print("  latest trade missing tx hash")
                    continue

                if latest_tx not in first_seen_by_tx:
                    first_seen_by_tx[latest_tx] = time.time()

                first_seen_age = time.time() - first_seen_by_tx[latest_tx]

                if last_seen_by_wallet.get(wallet) == latest_tx:
                    print(
                        f"  no new tx | latest_side={latest_side} | "
                        f"latest_age={latest_age} | first_seen_age={first_seen_age:.1f}s"
                    )
                    continue

                print(
                    f"  NEW TX DETECTED | latest_side={latest_side} | "
                    f"latest_age={latest_age} | first_seen_age={first_seen_age:.1f}s"
                )
                last_seen_by_wallet[wallet] = latest_tx

                try:
                    signal, snapshot, summary = await asyncio.to_thread(
                        latest_fresh_copyable_signal_from_wallet,
                        wallet,
                        leader_budget_usd,
                    )
                except Exception as e:
                    print(f"  source_error: {e}")
                    continue

                if signal is None:
                    latest_status = summary["latest_status"]

                    if latest_status == "TOO_OLD" and first_seen_age < poll_interval_sec + 1:
                        latest_status = "INDEXED_LATE"

                    print(
                        f"  no copyable signal | latest_side={summary['latest_trade_side']} | "
                        f"latest_age={summary['latest_trade_age_sec']} | "
                        f"first_seen_age={first_seen_age:.1f}s | "
                        f"status={latest_status} | reason={summary['latest_reason']}"
                    )
                    continue

                print("  SIGNAL FOUND")
                pprint({
                    "signal": signal,
                    "snapshot": snapshot,
                    "summary": summary,
                    "first_seen_age_sec": round(first_seen_age, 3),
                })

                if summary["selected_status"] == "LATE_BUT_COPYABLE":
                    print(
                        f"  selected_status=LATE_BUT_COPYABLE | "
                        f"selected_trade_age={summary['selected_trade_age_sec']} | "
                        f"preferred_window={preferred_signal_age_sec}s | "
                        f"max_window={max_signal_age_sec}s"
                    )

                try:
                    result = await asyncio.to_thread(process_signal, signal)
                    print("  PROCESS RESULT")
                    pprint(result)
                except Exception as e:
                    print(f"  process_error: {e}")

            pprint({"cycle_backup": backup_state_db(config=config, label="after_cycle")})
            sleep_sec = remaining_cycle_sleep_sec(
                cycle_started_monotonic=cycle_started_monotonic,
                interval_sec=poll_interval_sec,
            )
            if sleep_sec > 0:
                await asyncio.sleep(sleep_sec)

    except KeyboardInterrupt:
        print("\nStopped by user.")


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
