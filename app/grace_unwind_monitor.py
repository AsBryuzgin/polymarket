from __future__ import annotations

import csv
import sys
from datetime import datetime, timezone
from pathlib import Path
from pprint import pprint

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.position_marking import mark_position
from execution.state_store import init_db, list_open_positions, list_leader_registry


OUT_FILE = Path("data/grace_unwind_candidates.csv")


def parse_dt(value: str | None):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def save_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        print("No grace unwind candidates to save.")
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved: {path}")


def main() -> None:
    init_db()

    registry_rows = list_leader_registry(limit=100000)
    registry_by_wallet = {row["wallet"]: row for row in registry_rows}

    open_positions = list_open_positions(limit=100000)
    now = datetime.now(timezone.utc)

    rows = []

    for pos in open_positions:
        leader_wallet = pos["leader_wallet"]
        token_id = pos["token_id"]
        registry = registry_by_wallet.get(leader_wallet)

        if not registry:
            continue

        leader_status = registry.get("leader_status")
        if leader_status != "EXIT_ONLY":
            continue

        grace_until_raw = registry.get("grace_until")
        grace_until = parse_dt(grace_until_raw)

        if grace_until is None:
            grace_status = "NO_GRACE"
            days_to_grace_end = ""
            eligible_for_force_unwind = True
        else:
            delta_sec = (grace_until - now).total_seconds()
            days_to_grace_end = round(delta_sec / 86400, 4)

            if delta_sec > 0:
                grace_status = "GRACE_ACTIVE"
                eligible_for_force_unwind = False
            else:
                grace_status = "GRACE_EXPIRED"
                eligible_for_force_unwind = True

        marked = mark_position(pos)

        rows.append({
            "leader_wallet": leader_wallet,
            "leader_user_name": registry.get("user_name"),
            "category": registry.get("category"),
            "leader_status": leader_status,
            "token_id": token_id,
            "position_usd": pos.get("position_usd"),
            "avg_entry_price": pos.get("avg_entry_price"),
            "opened_at": pos.get("opened_at"),
            "updated_at": pos.get("updated_at"),
            "grace_until": grace_until_raw,
            "grace_status": grace_status,
            "days_to_grace_end": days_to_grace_end,
            "eligible_for_force_unwind": eligible_for_force_unwind,
            "best_bid": marked.get("best_bid"),
            "midpoint": marked.get("midpoint"),
            "best_ask": marked.get("best_ask"),
            "snapshot_status": marked.get("snapshot_status"),
            "snapshot_reason": marked.get("snapshot_reason"),
            "mark_source": marked.get("mark_source"),
            "settlement_price": marked.get("settlement_price"),
        })

    rows.sort(
        key=lambda r: (
            0 if r["eligible_for_force_unwind"] else 1,
            str(r.get("days_to_grace_end")),
        )
    )

    print("=== GRACE UNWIND MONITOR ===")
    pprint(rows)

    save_csv(rows, OUT_FILE)


if __name__ == "__main__":
    main()
