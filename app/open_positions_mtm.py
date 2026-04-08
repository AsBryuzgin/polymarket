from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path
from pprint import pprint

from execution.polymarket_executor import fetch_market_snapshot
from execution.state_store import init_db, list_open_positions, get_leader_registry


OUT_DETAIL = Path("data/open_positions_mtm.csv")
OUT_LEADER = Path("data/open_positions_mtm_by_leader.csv")
OUT_CATEGORY = Path("data/open_positions_mtm_by_category.csv")


def safe_float(x) -> float:
    try:
        return float(x) if x is not None else 0.0
    except Exception:
        return 0.0


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


def summarize(rows: list[dict], key_field: str) -> list[dict]:
    grouped = defaultdict(list)
    for row in rows:
        grouped[row.get(key_field) or "UNKNOWN"].append(row)

    out = []
    for key, items in grouped.items():
        invested = sum(safe_float(r.get("position_usd")) for r in items)
        mid_value = sum(safe_float(r.get("mark_value_mid_usd")) for r in items)
        bid_value = sum(safe_float(r.get("mark_value_bid_usd")) for r in items)
        pnl_mid = sum(safe_float(r.get("unrealized_pnl_mid_usd")) for r in items)
        pnl_bid = sum(safe_float(r.get("unrealized_pnl_bid_usd")) for r in items)

        out.append({
            key_field: key,
            "positions": len(items),
            "invested_usd": round(invested, 4),
            "mark_value_mid_usd": round(mid_value, 4),
            "mark_value_bid_usd": round(bid_value, 4),
            "unrealized_pnl_mid_usd": round(pnl_mid, 4),
            "unrealized_pnl_bid_usd": round(pnl_bid, 4),
        })

    out.sort(key=lambda r: r["unrealized_pnl_mid_usd"], reverse=True)
    return out


def main() -> None:
    init_db()
    positions = list_open_positions(limit=100000)

    if not positions:
        print("No open positions.")
        return

    detail_rows: list[dict] = []

    for pos in positions:
        leader_wallet = pos["leader_wallet"]
        token_id = pos["token_id"]
        position_usd = safe_float(pos["position_usd"])
        avg_entry_price = safe_float(pos["avg_entry_price"])
        opened_at = pos.get("opened_at")
        updated_at = pos.get("updated_at")

        registry = get_leader_registry(leader_wallet)
        leader_user_name = registry["user_name"] if registry else None
        category = registry["category"] if registry else None
        leader_status = registry["leader_status"] if registry else None

        qty = position_usd / avg_entry_price if avg_entry_price > 0 else 0.0

        row = {
            "leader_wallet": leader_wallet,
            "leader_user_name": leader_user_name,
            "category": category,
            "leader_status": leader_status,
            "token_id": token_id,
            "position_usd": round(position_usd, 6),
            "avg_entry_price": round(avg_entry_price, 6) if avg_entry_price else "",
            "estimated_qty": round(qty, 6),
            "midpoint": "",
            "best_bid": "",
            "best_ask": "",
            "mark_value_mid_usd": "",
            "mark_value_bid_usd": "",
            "unrealized_pnl_mid_usd": "",
            "unrealized_pnl_bid_usd": "",
            "unrealized_pnl_mid_pct": "",
            "unrealized_pnl_bid_pct": "",
            "opened_at": opened_at,
            "updated_at": updated_at,
            "snapshot_status": "OK",
            "snapshot_reason": "",
        }

        try:
            snapshot = fetch_market_snapshot(token_id=token_id, side="BUY")
            midpoint = safe_float(snapshot.get("midpoint"))
            best_bid = safe_float(snapshot.get("best_bid"))
            best_ask = safe_float(snapshot.get("best_ask"))

            mark_value_mid = qty * midpoint if midpoint > 0 else 0.0
            mark_value_bid = qty * best_bid if best_bid > 0 else 0.0

            pnl_mid = mark_value_mid - position_usd
            pnl_bid = mark_value_bid - position_usd

            pnl_mid_pct = (pnl_mid / position_usd) if position_usd > 0 else 0.0
            pnl_bid_pct = (pnl_bid / position_usd) if position_usd > 0 else 0.0

            row.update({
                "midpoint": round(midpoint, 6),
                "best_bid": round(best_bid, 6),
                "best_ask": round(best_ask, 6),
                "mark_value_mid_usd": round(mark_value_mid, 6),
                "mark_value_bid_usd": round(mark_value_bid, 6),
                "unrealized_pnl_mid_usd": round(pnl_mid, 6),
                "unrealized_pnl_bid_usd": round(pnl_bid, 6),
                "unrealized_pnl_mid_pct": round(pnl_mid_pct, 6),
                "unrealized_pnl_bid_pct": round(pnl_bid_pct, 6),
            })
        except Exception as e:
            row["snapshot_status"] = "ERROR"
            row["snapshot_reason"] = str(e)

        detail_rows.append(row)

    by_leader = summarize(detail_rows, "leader_user_name")
    by_category = summarize(detail_rows, "category")

    save_csv(detail_rows, OUT_DETAIL)
    save_csv(by_leader, OUT_LEADER)
    save_csv(by_category, OUT_CATEGORY)

    print("\n=== OPEN POSITIONS MTM ===")
    pprint(detail_rows)

    print("\n=== MTM BY LEADER ===")
    pprint(by_leader)

    print("\n=== MTM BY CATEGORY ===")
    pprint(by_category)


if __name__ == "__main__":
    main()
