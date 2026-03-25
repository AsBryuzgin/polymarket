from __future__ import annotations

import os
import time
from pprint import pprint

from dotenv import load_dotenv

from execution.copy_worker import LeaderSignal, process_signal
from execution.state_store import init_db, list_recent_signals

load_dotenv()


def main() -> None:
    init_db()

    token_id = os.getenv("PREVIEW_TOKEN_ID", "").strip()
    leader_budget_usd = float(os.getenv("PREVIEW_LEADER_BUDGET_USD", "6.0"))
    side = os.getenv("PREVIEW_SIDE", "BUY").strip().upper()

    if not token_id:
        raise ValueError("PREVIEW_TOKEN_ID is empty in .env")

    signal = LeaderSignal(
        signal_id=f"demo-{int(time.time())}",
        leader_wallet="0xDEMOLEADER00000000000000000000000000000000",
        token_id=token_id,
        side=side,
        leader_budget_usd=leader_budget_usd,
    )

    result = process_signal(signal)

    print("=== Copy Worker Demo Result ===")
    pprint(result)

    print("\n=== Recent Stored Signals ===")
    pprint(list_recent_signals(limit=10))


if __name__ == "__main__":
    main()
