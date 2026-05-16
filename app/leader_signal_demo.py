import sys
from pathlib import Path
from pprint import pprint

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.leader_signal_source import latest_buy_signal_from_wallet


def main() -> None:
    leader_wallet = "0x24c8cf69a0e0a17eee21f69d29752bfa32e823e1"
    leader_budget_usd = 5.61

    signal = latest_buy_signal_from_wallet(
        wallet=leader_wallet,
        leader_budget_usd=leader_budget_usd,
        limit=20,
    )

    print("=== Leader Signal Demo ===")
    pprint(signal)


if __name__ == "__main__":
    main()
