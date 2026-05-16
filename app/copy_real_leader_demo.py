import sys
from pathlib import Path
from pprint import pprint

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.copy_worker import process_signal
from execution.leader_signal_source import latest_fresh_copyable_signal_from_wallet
from execution.state_store import init_db


def main() -> None:
    init_db()

    leader_wallet = "0x24c8cf69a0e0a17eee21f69d29752bfa32e823e1"
    leader_budget_usd = 5.61

    signal, snapshot, diagnostics = latest_fresh_copyable_signal_from_wallet(
        wallet=leader_wallet,
        leader_budget_usd=leader_budget_usd,
    )

    print("=== Fresh Signal Diagnostics ===")
    pprint(diagnostics)

    if signal is None:
        print("\nNo fresh copyable signal found for this leader")
        return

    print("\n=== Selected Signal Snapshot ===")
    pprint(snapshot)

    result = process_signal(signal)

    print("\n=== Copy Real Leader Demo ===")
    pprint(result)


if __name__ == "__main__":
    main()
