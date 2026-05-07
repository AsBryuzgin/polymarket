from __future__ import annotations

import csv
import sys
from collections import defaultdict
from pathlib import Path
from pprint import pprint

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.position_marking import mark_position
from execution.state_store import (
    init_db,
    list_open_positions,
    list_trade_history,
    get_leader_registry,
)


OUT_OVERVIEW = Path("data/portfolio_summary_overview.csv")
OUT_LEADER = Path("data/portfolio_summary_by_leader.csv")
OUT_CATEGORY = Path("data/portfolio_summary_by_category.csv")


def safe_float(x) -> float:
    try:
        return float(x) if x is not None else 0.0
    except Exception:
        return 0.0


def safe_int(x) -> int:
    try:
        return int(x) if x is not None else 0
    except Exception:
        return 0


def save_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        print(f"No rows to save for {path}")
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved: {path}")


def build_open_position_rows() -> list[dict]:
    positions = list_open_positions(limit=100000)
    rows: list[dict] = []

    for pos in positions:
        leader_wallet = pos["leader_wallet"]
        token_id = pos["token_id"]
        position_usd = safe_float(pos["position_usd"])
        avg_entry_price = safe_float(pos["avg_entry_price"])

        registry = get_leader_registry(leader_wallet)
        leader_user_name = registry["user_name"] if registry else "UNKNOWN"
        category = registry["category"] if registry else "UNKNOWN"
        leader_status = registry["leader_status"] if registry else "UNKNOWN"

        marked = mark_position(pos)
        qty = safe_float(marked.get("qty"))

        row = {
            "leader_wallet": leader_wallet,
            "leader_user_name": leader_user_name,
            "category": category,
            "leader_status": leader_status,
            "token_id": token_id,
            "position_usd": position_usd,
            "avg_entry_price": avg_entry_price,
            "estimated_qty": qty,
            "mark_value_mid_usd": 0.0,
            "mark_value_bid_usd": 0.0,
            "unrealized_pnl_mid_usd": 0.0,
            "unrealized_pnl_bid_usd": 0.0,
            "snapshot_status": marked.get("snapshot_status") or "OK",
            "snapshot_reason": marked.get("snapshot_reason") or "",
            "mark_source": marked.get("mark_source") or "",
            "settlement_price": marked.get("settlement_price") or "",
        }

        mark_value_mid = marked.get("mark_value_mid_usd")
        mark_value_bid = marked.get("mark_value_bid_usd")
        if mark_value_mid is not None:
            row["mark_value_mid_usd"] = safe_float(mark_value_mid)
            row["mark_value_bid_usd"] = safe_float(mark_value_bid)
            row["unrealized_pnl_mid_usd"] = safe_float(marked.get("unrealized_pnl_mid_usd"))
            row["unrealized_pnl_bid_usd"] = safe_float(marked.get("unrealized_pnl_bid_usd"))

        rows.append(row)

    return rows


def build_realized_maps(history_rows: list[dict], key_field: str) -> dict[str, dict]:
    grouped = defaultdict(list)
    for row in history_rows:
        grouped[row.get(key_field) or "UNKNOWN"].append(row)

    out: dict[str, dict] = {}
    for key, items in grouped.items():
        entries = [r for r in items if r.get("event_type") == "ENTRY"]
        exits = [r for r in items if r.get("event_type") == "EXIT"]

        realized_pnl = sum(
            safe_float(r.get("realized_pnl_usd"))
            for r in exits
            if r.get("realized_pnl_usd") is not None
        )
        holding_vals = [
            safe_float(r.get("holding_minutes"))
            for r in exits
            if r.get("holding_minutes") is not None
        ]
        win_count = sum(
            1 for r in exits
            if r.get("realized_pnl_usd") is not None and safe_float(r.get("realized_pnl_usd")) > 0
        )
        total_exits = len(exits)

        out[key] = {
            "entries": len(entries),
            "exits": len(exits),
            "realized_pnl_usd": realized_pnl,
            "win_count": win_count,
            "win_rate": (win_count / total_exits) if total_exits > 0 else None,
            "avg_holding_minutes": (
                sum(holding_vals) / len(holding_vals) if holding_vals else None
            ),
        }

    return out


def summarize_positions(rows: list[dict], key_field: str) -> dict[str, dict]:
    grouped = defaultdict(list)
    for row in rows:
        grouped[row.get(key_field) or "UNKNOWN"].append(row)

    out: dict[str, dict] = {}
    for key, items in grouped.items():
        out[key] = {
            "open_positions": len(items),
            "invested_open_usd": sum(safe_float(r.get("position_usd")) for r in items),
            "mark_value_mid_usd": sum(safe_float(r.get("mark_value_mid_usd")) for r in items),
            "mark_value_bid_usd": sum(safe_float(r.get("mark_value_bid_usd")) for r in items),
            "unrealized_pnl_mid_usd": sum(safe_float(r.get("unrealized_pnl_mid_usd")) for r in items),
            "unrealized_pnl_bid_usd": sum(safe_float(r.get("unrealized_pnl_bid_usd")) for r in items),
        }
    return out


def merge_summary(realized_map: dict[str, dict], open_map: dict[str, dict], key_field: str) -> list[dict]:
    keys = sorted(set(realized_map.keys()) | set(open_map.keys()))
    rows = []

    for key in keys:
        r = realized_map.get(key, {})
        o = open_map.get(key, {})

        realized = safe_float(r.get("realized_pnl_usd"))
        unreal_mid = safe_float(o.get("unrealized_pnl_mid_usd"))
        unreal_bid = safe_float(o.get("unrealized_pnl_bid_usd"))

        rows.append({
            key_field: key,
            "entries": safe_int(r.get("entries")),
            "exits": safe_int(r.get("exits")),
            "open_positions": safe_int(o.get("open_positions")),
            "invested_open_usd": round(safe_float(o.get("invested_open_usd")), 4),
            "mark_value_mid_usd": round(safe_float(o.get("mark_value_mid_usd")), 4),
            "mark_value_bid_usd": round(safe_float(o.get("mark_value_bid_usd")), 4),
            "realized_pnl_usd": round(realized, 4),
            "unrealized_pnl_mid_usd": round(unreal_mid, 4),
            "unrealized_pnl_bid_usd": round(unreal_bid, 4),
            "total_pnl_mid_usd": round(realized + unreal_mid, 4),
            "total_pnl_bid_usd": round(realized + unreal_bid, 4),
            "win_count": safe_int(r.get("win_count")),
            "win_rate": round(r["win_rate"], 6) if r.get("win_rate") is not None else "",
            "avg_holding_minutes": round(r["avg_holding_minutes"], 4) if r.get("avg_holding_minutes") is not None else "",
        })

    rows.sort(key=lambda x: x["total_pnl_mid_usd"], reverse=True)
    return rows


def build_overview(history_rows: list[dict], open_rows: list[dict]) -> list[dict]:
    entries = [r for r in history_rows if r.get("event_type") == "ENTRY"]
    exits = [r for r in history_rows if r.get("event_type") == "EXIT"]

    realized = sum(
        safe_float(r.get("realized_pnl_usd"))
        for r in exits
        if r.get("realized_pnl_usd") is not None
    )

    unreal_mid = sum(safe_float(r.get("unrealized_pnl_mid_usd")) for r in open_rows)
    unreal_bid = sum(safe_float(r.get("unrealized_pnl_bid_usd")) for r in open_rows)
    invested_open = sum(safe_float(r.get("position_usd")) for r in open_rows)
    mark_mid = sum(safe_float(r.get("mark_value_mid_usd")) for r in open_rows)
    mark_bid = sum(safe_float(r.get("mark_value_bid_usd")) for r in open_rows)

    win_count = sum(
        1 for r in exits
        if r.get("realized_pnl_usd") is not None and safe_float(r.get("realized_pnl_usd")) > 0
    )
    total_exits = len(exits)
    win_rate = (win_count / total_exits) if total_exits > 0 else None

    unique_leaders = len({r.get("leader_user_name") for r in open_rows if r.get("leader_user_name")})
    unique_categories = len({r.get("category") for r in open_rows if r.get("category")})

    overview = [{
        "entries": len(entries),
        "exits": len(exits),
        "open_positions": len(open_rows),
        "unique_open_leaders": unique_leaders,
        "unique_open_categories": unique_categories,
        "invested_open_usd": round(invested_open, 4),
        "mark_value_mid_usd": round(mark_mid, 4),
        "mark_value_bid_usd": round(mark_bid, 4),
        "realized_pnl_usd": round(realized, 4),
        "unrealized_pnl_mid_usd": round(unreal_mid, 4),
        "unrealized_pnl_bid_usd": round(unreal_bid, 4),
        "total_pnl_mid_usd": round(realized + unreal_mid, 4),
        "total_pnl_bid_usd": round(realized + unreal_bid, 4),
        "win_count": win_count,
        "win_rate": round(win_rate, 6) if win_rate is not None else "",
    }]
    return overview


def main() -> None:
    init_db()

    history_rows = list_trade_history(limit=100000)
    open_rows = build_open_position_rows()

    realized_by_leader = build_realized_maps(history_rows, "leader_user_name")
    realized_by_category = build_realized_maps(history_rows, "category")

    open_by_leader = summarize_positions(open_rows, "leader_user_name")
    open_by_category = summarize_positions(open_rows, "category")

    overview = build_overview(history_rows, open_rows)
    by_leader = merge_summary(realized_by_leader, open_by_leader, "leader_user_name")
    by_category = merge_summary(realized_by_category, open_by_category, "category")

    save_csv(overview, OUT_OVERVIEW)
    save_csv(by_leader, OUT_LEADER)
    save_csv(by_category, OUT_CATEGORY)

    print("\n=== PORTFOLIO OVERVIEW ===")
    pprint(overview)

    print("\n=== PORTFOLIO BY LEADER ===")
    pprint(by_leader)

    print("\n=== PORTFOLIO BY CATEGORY ===")
    pprint(by_category)


if __name__ == "__main__":
    main()
