from __future__ import annotations

import csv
import json
import math
import tomllib
from collections import defaultdict
from pathlib import Path


FINAL_FILE = Path("data/shortlists/final_portfolio_allocation.csv")
CURRENT_LIVE_FILE = Path("data/shortlists/live_portfolio_allocation.csv")
OUTPUT_LIVE_FILE = Path("data/shortlists/live_portfolio_allocation.csv")
REPORT_FILE = Path("data/shortlists/live_rebalance_report.csv")
STATE_FILE = Path("data/rebalance_state.json")
CONFIG_FILE = Path("config/rebalance.toml")


def load_toml(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open("rb") as f:
        return tomllib.load(f)


def load_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    for row in rows:
        if "weight" in row and row["weight"] not in (None, ""):
            row["weight"] = float(row["weight"])
        if "final_wss" in row and row["final_wss"] not in (None, ""):
            row["final_wss"] = float(row["final_wss"])
        if "leaderboard_pnl" in row and row["leaderboard_pnl"] not in (None, ""):
            row["leaderboard_pnl"] = float(row["leaderboard_pnl"])
        if "leaderboard_volume" in row and row["leaderboard_volume"] not in (None, ""):
            row["leaderboard_volume"] = float(row["leaderboard_volume"])

    return rows


def save_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        return

    fieldnames = list(rows[0].keys())
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def load_state(path: Path) -> dict:
    if not path.exists():
        return {"categories": {}}
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(state: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def group_by_category(rows: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        grouped[row["category"]].append(row)
    for category in grouped:
        grouped[category].sort(key=lambda x: x["final_wss"], reverse=True)
    return grouped


def index_live_by_category(rows: list[dict]) -> dict[str, dict]:
    return {row["category"]: row for row in rows}


def find_rank(rows: list[dict], wallet: str) -> int | None:
    for idx, row in enumerate(rows, start=1):
        if row["wallet"] == wallet:
            return idx
    return None


def pick_selected_weight_base(row: dict) -> float:
    # Use precomputed portfolio weight if present, otherwise fallback to WSS.
    weight = row.get("weight")
    if isinstance(weight, (int, float)) and weight > 0:
        return float(weight)
    return float(row["final_wss"])


def main() -> None:
    cfg = load_toml(CONFIG_FILE).get("rebalance", {})
    exclude_categories = set(cfg.get("exclude_categories", ["MENTIONS"]))
    incumbent_max_rank = int(cfg.get("incumbent_max_rank", 2))
    hold_gap_abs = float(cfg.get("hold_gap_abs", 4.0))
    hold_gap_rel = float(cfg.get("hold_gap_rel", 0.05))
    confirmation_cycles = int(cfg.get("confirmation_cycles", 2))

    final_rows = [r for r in load_csv(FINAL_FILE) if r["category"] not in exclude_categories]
    if not final_rows:
        raise FileNotFoundError(f"No rows found in {FINAL_FILE}")

    current_live_rows = [r for r in load_csv(CURRENT_LIVE_FILE) if r["category"] not in exclude_categories]
    grouped = group_by_category(final_rows)
    current_live = index_live_by_category(current_live_rows)
    state = load_state(STATE_FILE)
    category_state = state.setdefault("categories", {})

    selected_rows: list[dict] = []
    report_rows: list[dict] = []

    for category, candidates in grouped.items():
        incumbent = current_live.get(category)
        top1 = candidates[0]
        top2 = candidates[1] if len(candidates) > 1 else None

        incumbent_wallet = incumbent["wallet"] if incumbent else None
        incumbent_rank = find_rank(candidates, incumbent_wallet) if incumbent_wallet else None

        top1_wallet = top1["wallet"]
        top1_wss = float(top1["final_wss"])

        chosen = None
        decision = None
        reason = None
        gap_abs = None
        gap_rel = None
        pending_count = 0

        cat_state = category_state.setdefault(category, {
            "pending_challenger_wallet": None,
            "pending_challenger_count": 0,
            "previous_incumbent_wallet": incumbent_wallet,
        })

        if incumbent is None:
            chosen = top1
            decision = "ADD_NEW"
            reason = "no incumbent in category"
            cat_state["pending_challenger_wallet"] = None
            cat_state["pending_challenger_count"] = 0

        elif top1_wallet == incumbent_wallet:
            chosen = incumbent
            decision = "KEEP"
            reason = "incumbent remains rank #1"
            cat_state["pending_challenger_wallet"] = None
            cat_state["pending_challenger_count"] = 0

        else:
            incumbent_row = next((r for r in candidates if r["wallet"] == incumbent_wallet), None)

            if incumbent_row is None or incumbent_rank is None:
                chosen = top1
                decision = "REPLACE"
                reason = "incumbent missing from candidate set"
                cat_state["pending_challenger_wallet"] = None
                cat_state["pending_challenger_count"] = 0
            elif incumbent_rank > incumbent_max_rank:
                chosen = top1
                decision = "REPLACE"
                reason = f"incumbent rank {incumbent_rank} worse than allowed max rank {incumbent_max_rank}"
                cat_state["pending_challenger_wallet"] = None
                cat_state["pending_challenger_count"] = 0
            else:
                incumbent_wss = float(incumbent_row["final_wss"])
                gap_abs = top1_wss - incumbent_wss
                gap_rel = gap_abs / top1_wss if top1_wss > 0 else math.inf

                if gap_abs <= hold_gap_abs or gap_rel <= hold_gap_rel:
                    chosen = incumbent_row
                    decision = "HOLD_INCUMBENT"
                    reason = (
                        f"incumbent still top-{incumbent_max_rank} and gap small "
                        f"(gap_abs={gap_abs:.2f}, gap_rel={gap_rel:.2%})"
                    )
                    cat_state["pending_challenger_wallet"] = None
                    cat_state["pending_challenger_count"] = 0
                else:
                    prev_pending_wallet = cat_state.get("pending_challenger_wallet")
                    prev_pending_count = int(cat_state.get("pending_challenger_count", 0))

                    if prev_pending_wallet == top1_wallet:
                        pending_count = prev_pending_count + 1
                    else:
                        pending_count = 1

                    if pending_count >= confirmation_cycles:
                        chosen = top1
                        decision = "REPLACE_CONFIRMED"
                        reason = (
                            f"challenger confirmed for {pending_count} consecutive rebalances "
                            f"(gap_abs={gap_abs:.2f}, gap_rel={gap_rel:.2%})"
                        )
                        cat_state["pending_challenger_wallet"] = None
                        cat_state["pending_challenger_count"] = 0
                    else:
                        chosen = incumbent_row
                        decision = "PENDING_REPLACE"
                        reason = (
                            f"challenger leads materially but needs confirmation "
                            f"{pending_count}/{confirmation_cycles}"
                        )
                        cat_state["pending_challenger_wallet"] = top1_wallet
                        cat_state["pending_challenger_count"] = pending_count

        cat_state["previous_incumbent_wallet"] = chosen["wallet"]
        selected_rows.append(dict(chosen))

        report_rows.append({
            "category": category,
            "decision": decision,
            "reason": reason,
            "incumbent_wallet": incumbent_wallet,
            "incumbent_user_name": incumbent["user_name"] if incumbent else "",
            "incumbent_rank": incumbent_rank if incumbent_rank is not None else "",
            "incumbent_wss": (
                next((r["final_wss"] for r in candidates if r["wallet"] == incumbent_wallet), "")
                if incumbent_wallet else ""
            ),
            "top1_wallet": top1_wallet,
            "top1_user_name": top1["user_name"],
            "top1_wss": round(top1_wss, 2),
            "top2_wallet": top2["wallet"] if top2 else "",
            "top2_user_name": top2["user_name"] if top2 else "",
            "top2_wss": round(float(top2["final_wss"]), 2) if top2 else "",
            "gap_abs": round(gap_abs, 4) if gap_abs is not None else "",
            "gap_rel": round(gap_rel, 6) if gap_rel is not None else "",
            "pending_count": pending_count,
            "selected_wallet": chosen["wallet"],
            "selected_user_name": chosen["user_name"],
            "selected_wss": round(float(chosen["final_wss"]), 2),
        })

    # re-normalize weights only across selected live universe
    total_base = sum(pick_selected_weight_base(r) for r in selected_rows)
    for row in selected_rows:
        base = pick_selected_weight_base(row)
        row["weight"] = round(base / total_base, 6) if total_base > 0 else 0.0

    selected_rows.sort(key=lambda x: x["weight"], reverse=True)
    report_rows.sort(key=lambda x: x["category"])

    save_csv(selected_rows, OUTPUT_LIVE_FILE)
    save_csv(report_rows, REPORT_FILE)
    save_state(state, STATE_FILE)

    print("=== STABLE LIVE UNIVERSE ===")
    for row in selected_rows:
        print(
            f"{row['category']}: {row['user_name']} | "
            f"wss={row['final_wss']:.2f} | weight={row['weight']:.4f} | wallet={row['wallet']}"
        )

    print(f"\nSaved live universe: {OUTPUT_LIVE_FILE}")
    print(f"Saved rebalance report: {REPORT_FILE}")
    print(f"Saved rebalance state: {STATE_FILE}")


if __name__ == "__main__":
    main()
