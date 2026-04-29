from __future__ import annotations

import csv
import json
import shutil
import sys
import zipfile
from contextlib import redirect_stdout
from datetime import datetime, timezone
from html import escape
from io import StringIO
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import app.build_live_universe_stable as stable_universe
from app import final_portfolio_candidates_demo, multi_category_shortlist_demo, portfolio_allocation_demo
from app.apply_rebalance_lifecycle import main as apply_rebalance_lifecycle


SHORTLIST_DIR = Path("data/shortlists")
REVIEW_DIR = Path("data/rebalance_reviews")
PENDING_FILE = REVIEW_DIR / "pending_review.json"
MASTER_CORE_FILE = SHORTLIST_DIR / "master_shortlist_core.csv"
MASTER_EXPERIMENTAL_FILE = SHORTLIST_DIR / "master_shortlist_experimental.csv"
FINAL_CANDIDATES_FILE = SHORTLIST_DIR / "final_portfolio_candidates.csv"
FINAL_ALLOCATION_FILE = SHORTLIST_DIR / "final_portfolio_allocation.csv"
LIVE_FILE = SHORTLIST_DIR / "live_portfolio_allocation.csv"
REPORT_FILE = SHORTLIST_DIR / "live_rebalance_report.csv"
STATE_FILE = Path("data/rebalance_state.json")
SCORING_VERSION = "wss_v2_hard_gates_2026_04_29"

REVIEW_COLUMNS = [
    "category",
    "time_period",
    "rank",
    "user_name",
    "wallet",
    "leaderboard_pnl",
    "leaderboard_week_pnl",
    "leaderboard_month_pnl",
    "leaderboard_volume",
    "eligible",
    "filter_reasons",
    "final_wss",
    "raw_wss",
    "formula_raw_wss",
    "formula_final_wss",
    "consistency_score",
    "drawdown_score",
    "specialization_score",
    "copyability_score",
    "copyability_score_raw",
    "copyability_smoothing_samples",
    "return_quality_score",
    "track_record_multiplier",
    "data_depth_multiplier",
    "activity_score",
    "current_position_pnl_ratio",
    "roi_7",
    "roi_30",
    "trades_30d",
    "trades_90d",
    "buy_trades_30d",
    "sell_trades_30d",
    "buy_trade_share_30d",
    "days_since_last_trade",
    "median_spread",
    "median_liquidity",
    "slippage_proxy",
    "closed_positions_used",
]

REQUIRED_SCORING_COLUMNS = [
    "consistency_score",
    "drawdown_score",
    "specialization_score",
    "copyability_score",
    "copyability_score_raw",
    "copyability_smoothing_samples",
    "return_quality_score",
    "track_record_multiplier",
    "data_depth_multiplier",
    "activity_score",
    "current_position_pnl_ratio",
    "roi_7",
    "roi_30",
    "trades_30d",
    "trades_90d",
    "buy_trades_30d",
    "sell_trades_30d",
    "buy_trade_share_30d",
    "days_since_last_trade",
    "median_spread",
    "median_liquidity",
    "slippage_proxy",
    "closed_positions_used",
    "leaderboard_week_pnl",
    "leaderboard_month_pnl",
]


def _utc_review_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(rows: list[dict[str, Any]], path: Path, fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        seen: list[str] = []
        for row in rows:
            for key in row:
                if key not in seen:
                    seen.append(key)
        fieldnames = seen
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _column_name(index: int) -> str:
    name = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        name = chr(65 + remainder) + name
    return name


def _cell_xml(ref: str, value: Any) -> str:
    if isinstance(value, str) and value.startswith("="):
        return f'<c r="{ref}"><f>{escape(value[1:])}</f></c>'
    if isinstance(value, bool):
        return f'<c r="{ref}" t="b"><v>{1 if value else 0}</v></c>'
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return f'<c r="{ref}"><v>{value}</v></c>'
    if value is None:
        value = ""
    return f'<c r="{ref}" t="inlineStr"><is><t>{escape(str(value))}</t></is></c>'


def _sheet_xml(rows: list[list[Any]]) -> str:
    row_xml = []
    for row_idx, row in enumerate(rows, start=1):
        cells = []
        for col_idx, value in enumerate(row, start=1):
            cells.append(_cell_xml(f"{_column_name(col_idx)}{row_idx}", value))
        row_xml.append(f'<row r="{row_idx}">{"".join(cells)}</row>')
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<sheetViews><sheetView workbookViewId="0"><pane ySplit="1" topLeftCell="A2" '
        'activePane="bottomLeft" state="frozen"/></sheetView></sheetViews>'
        f"<sheetData>{''.join(row_xml)}</sheetData>"
        "</worksheet>"
    )


def _workbook_xml(sheet_names: list[str]) -> str:
    sheets = "".join(
        f'<sheet name="{escape(name)}" sheetId="{idx}" r:id="rId{idx}"/>'
        for idx, name in enumerate(sheet_names, start=1)
    )
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        f"<sheets>{sheets}</sheets>"
        '<calcPr calcId="0" fullCalcOnLoad="1" forceFullCalc="1"/>'
        "</workbook>"
    )


def _workbook_rels_xml(sheet_count: int) -> str:
    rels = [
        f'<Relationship Id="rId{idx}" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        f'Target="worksheets/sheet{idx}.xml"/>'
        for idx in range(1, sheet_count + 1)
    ]
    rels.append(
        f'<Relationship Id="rId{sheet_count + 1}" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" '
        'Target="styles.xml"/>'
    )
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        f"{''.join(rels)}</Relationships>"
    )


def _content_types_xml(sheet_count: int) -> str:
    sheets = "".join(
        '<Override PartName="/xl/worksheets/sheet{idx}.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'.format(idx=idx)
        for idx in range(1, sheet_count + 1)
    )
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/styles.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
        f"{sheets}</Types>"
    )


def _root_rels_xml() -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/>'
        "</Relationships>"
    )


def _styles_xml() -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>'
        '<fills count="1"><fill><patternFill patternType="none"/></fill></fills>'
        '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>'
        '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
        '<cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs>'
        "</styleSheet>"
    )


def _review_rows_with_formulas(rows: list[dict[str, Any]]) -> list[list[Any]]:
    header = REVIEW_COLUMNS
    indexes = {name: idx + 1 for idx, name in enumerate(header)}
    out = [header]
    for row_idx, row in enumerate(rows, start=2):
        raw_formula = (
            f"=0.35*{_column_name(indexes['consistency_score'])}{row_idx}"
            f"+0.25*{_column_name(indexes['drawdown_score'])}{row_idx}"
            f"+0.20*{_column_name(indexes['specialization_score'])}{row_idx}"
            f"+0.10*{_column_name(indexes['copyability_score'])}{row_idx}"
            f"+0.10*{_column_name(indexes['return_quality_score'])}{row_idx}"
        )
        final_formula = (
            f"={_column_name(indexes['formula_raw_wss'])}{row_idx}"
            f"*{_column_name(indexes['track_record_multiplier'])}{row_idx}"
            f"*{_column_name(indexes['data_depth_multiplier'])}{row_idx}"
        )
        enriched = dict(row)
        enriched["formula_raw_wss"] = raw_formula
        enriched["formula_final_wss"] = final_formula
        out.append([enriched.get(col, "") for col in header])
    return out


def write_review_xlsx(rows: list[dict[str, Any]], path: Path) -> None:
    formula_rows = [
        ["metric", "weight", "meaning"],
        ["consistency_score", 0.35, "ROI sign, ROI stability, median monthly ROI"],
        ["drawdown_score", 0.25, "max drawdown, losing streak, downside volatility"],
        ["specialization_score", 0.20, "domain focus and single-market concentration penalty"],
        ["copyability_score", 0.10, "spread, liquidity, slippage proxy, delay"],
        ["return_quality_score", 0.10, "ROI 180, profit factor, largest-win dependency"],
        ["raw_wss", "weighted strategy quality score", "does not include track-record confidence haircut"],
        [
            "final_wss",
            "raw_wss * track_record_multiplier * data_depth_multiplier",
            "confidence-adjusted WSS used for ranking/allocation",
        ],
        [
            "activity_score",
            "display only; not included in WSS",
            "activity is enforced by hard gates: trades30>=30 and last_trade<=5d",
        ],
        [
            "recent PnL gates",
            "gate only; not included in WSS",
            "leader must have positive week and month signal from leaderboard PnL or closed-position ROI",
        ],
        [
            "copy-flow filter",
            "display/gate only; not included in WSS",
            "rejects SELL-only or near SELL-only recent taker flow that cannot open copy entries",
        ],
        [
            "hard gates",
            "age>=120, closed>=40, unique>=15, concentration<=35%, open_pnl>=-10%, trades30>=30, last_trade<=5d, copyability>=60, positive week/month PnL, copy-flow buy presence",
            "",
        ],
    ]
    sheets = {
        "Top30": _review_rows_with_formulas(rows),
        "Formula": formula_rows,
    }

    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", _content_types_xml(len(sheets)))
        zf.writestr("_rels/.rels", _root_rels_xml())
        zf.writestr("xl/workbook.xml", _workbook_xml(list(sheets)))
        zf.writestr("xl/_rels/workbook.xml.rels", _workbook_rels_xml(len(sheets)))
        zf.writestr("xl/styles.xml", _styles_xml())
        for idx, sheet_rows in enumerate(sheets.values(), start=1):
            zf.writestr(f"xl/worksheets/sheet{idx}.xml", _sheet_xml(sheet_rows))


def _all_review_rows() -> list[dict[str, Any]]:
    rows = _read_csv(MASTER_CORE_FILE) + _read_csv(MASTER_EXPERIMENTAL_FILE)
    if not rows:
        raise FileNotFoundError("No master shortlist rows found. Run multi_category_shortlist_demo.py first.")
    return rows


def _missing_scoring_columns(rows: list[dict[str, Any]]) -> list[str]:
    missing = []
    for column in REQUIRED_SCORING_COLUMNS:
        if not any(str(row.get(column) or "").strip() for row in rows):
            missing.append(column)
    return missing


def _validate_review_rows(rows: list[dict[str, Any]]) -> None:
    missing = _missing_scoring_columns(rows)
    if missing:
        raise RuntimeError(
            "shortlist rows are missing scoring columns: "
            + ", ".join(missing)
            + ". Re-run the current multi-category shortlist build before creating review."
        )


def refresh_shortlists() -> str:
    buf = StringIO()
    with redirect_stdout(buf):
        multi_category_shortlist_demo.main()
    rows = _all_review_rows()
    _validate_review_rows(rows)
    return buf.getvalue()


def _copy_required(src: Path, dst: Path) -> None:
    if not src.exists():
        raise FileNotFoundError(f"Missing expected file: {src}")
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def _preview_stable_live_universe(
    *,
    final_allocation: Path,
    output_live: Path,
    output_report: Path,
    output_state: Path,
) -> str:
    output_state.parent.mkdir(parents=True, exist_ok=True)
    if STATE_FILE.exists():
        shutil.copy2(STATE_FILE, output_state)

    old_final = stable_universe.FINAL_FILE
    old_current = stable_universe.CURRENT_LIVE_FILE
    old_output = stable_universe.OUTPUT_LIVE_FILE
    old_report = stable_universe.REPORT_FILE
    old_state = stable_universe.STATE_FILE
    try:
        stable_universe.FINAL_FILE = final_allocation
        stable_universe.CURRENT_LIVE_FILE = LIVE_FILE
        stable_universe.OUTPUT_LIVE_FILE = output_live
        stable_universe.REPORT_FILE = output_report
        stable_universe.STATE_FILE = output_state
        buf = StringIO()
        with redirect_stdout(buf):
            stable_universe.main()
        return buf.getvalue()
    finally:
        stable_universe.FINAL_FILE = old_final
        stable_universe.CURRENT_LIVE_FILE = old_current
        stable_universe.OUTPUT_LIVE_FILE = old_output
        stable_universe.REPORT_FILE = old_report
        stable_universe.STATE_FILE = old_state


def _pending_paths(review_id: str) -> dict[str, Path]:
    root = REVIEW_DIR / review_id
    return {
        "root": root,
        "all_csv": root / f"top30_all_categories_{review_id}.csv",
        "xlsx": root / f"top30_all_categories_{review_id}.xlsx",
        "final_candidates": root / "final_portfolio_candidates.csv",
        "final_allocation": root / "final_portfolio_allocation.csv",
        "live": root / "pending_live_portfolio_allocation.csv",
        "report": root / "pending_live_rebalance_report.csv",
        "state": root / "pending_rebalance_state.json",
    }


def _summarize_live_rows(rows: list[dict[str, str]]) -> str:
    if not rows:
        return "No proposed live universe rows."
    lines = []
    for idx, row in enumerate(rows, start=1):
        weight = _safe_float(row.get("weight")) * 100.0
        lines.append(
            f"{idx}. {row.get('user_name')} | {row.get('category')} | "
            f"WSS {row.get('final_wss')} | {weight:.2f}%"
        )
    return "\n".join(lines)


def create_rebalance_review(*, refresh: bool = True) -> dict[str, Any]:
    review_id = _utc_review_id()
    paths = _pending_paths(review_id)
    paths["root"].mkdir(parents=True, exist_ok=True)

    refresh_log = ""
    if refresh:
        refresh_log = refresh_shortlists()

    all_rows = _all_review_rows()
    _validate_review_rows(all_rows)
    _write_csv(all_rows, paths["all_csv"], REVIEW_COLUMNS)
    write_review_xlsx(all_rows, paths["xlsx"])

    with redirect_stdout(StringIO()):
        final_portfolio_candidates_demo.main()
        portfolio_allocation_demo.main()

    _copy_required(FINAL_CANDIDATES_FILE, paths["final_candidates"])
    _copy_required(FINAL_ALLOCATION_FILE, paths["final_allocation"])
    preview_log = _preview_stable_live_universe(
        final_allocation=paths["final_allocation"],
        output_live=paths["live"],
        output_report=paths["report"],
        output_state=paths["state"],
    )

    live_rows = _read_csv(paths["live"])
    review = {
        "review_id": review_id,
        "scoring_version": SCORING_VERSION,
        "status": "PENDING",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "manual_overrides": {},
        "files": {key: str(path) for key, path in paths.items() if key != "root"},
        "proposed_live": live_rows,
        "refresh_log_tail": refresh_log[-4000:],
        "preview_log_tail": preview_log[-4000:],
    }
    PENDING_FILE.parent.mkdir(parents=True, exist_ok=True)
    PENDING_FILE.write_text(json.dumps(review, ensure_ascii=False, indent=2), encoding="utf-8")
    return review


def load_pending_review() -> dict[str, Any] | None:
    if not PENDING_FILE.exists():
        return None
    review = json.loads(PENDING_FILE.read_text(encoding="utf-8"))
    if review.get("status") != "PENDING":
        return None
    if review.get("scoring_version") != SCORING_VERSION:
        return None
    return review


def _set_pending_review(review: dict[str, Any]) -> None:
    PENDING_FILE.parent.mkdir(parents=True, exist_ok=True)
    PENDING_FILE.write_text(json.dumps(review, ensure_ascii=False, indent=2), encoding="utf-8")


def build_review_message(review: dict[str, Any]) -> str:
    return (
        "Rebalance review готов\n"
        f"id: {review['review_id']}\n\n"
        "Предложенный live-universe:\n"
        f"{_summarize_live_rows(review.get('proposed_live') or [])}\n\n"
        "Файлы с top-30 и формулами приложены. Execution не меняется до подтверждения."
    )


def _live_fieldnames(rows: list[dict[str, Any]]) -> list[str]:
    preferred = [
        "user_name",
        "wallet",
        "category",
        "all_categories",
        "final_wss",
        "activity_score",
        "leaderboard_pnl",
        "leaderboard_week_pnl",
        "leaderboard_month_pnl",
        "leaderboard_volume",
        "raw_weight",
        "weight",
        "trades_30d",
        "trades_90d",
        "buy_trades_30d",
        "sell_trades_30d",
        "buy_trade_share_30d",
        "days_since_last_trade",
        "median_spread",
        "slippage_proxy",
        "current_position_pnl_ratio",
        "roi_7",
        "roi_30",
        "copyability_score_raw",
        "copyability_smoothing_samples",
    ]
    seen = list(preferred)
    for row in rows:
        for key in row:
            if key not in seen:
                seen.append(key)
    return seen


def _reweight_live_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    total = sum(max(_safe_float(row.get("final_wss")), 0.0) for row in rows)
    for row in rows:
        raw = max(_safe_float(row.get("final_wss")), 0.0) / total if total > 0 else 0.0
        row["raw_weight"] = round(raw, 6)
        row["weight"] = round(raw, 6)
    rows.sort(key=lambda row: _safe_float(row.get("weight")), reverse=True)
    return rows


def _candidate_rows_for_category(review: dict[str, Any], category: str) -> list[dict[str, str]]:
    all_csv = Path(review["files"]["all_csv"])
    rows = [
        row
        for row in _read_csv(all_csv)
        if str(row.get("category") or "").upper() == category.upper()
        and str(row.get("eligible") or "").lower() == "true"
    ]
    rows.sort(key=lambda row: _safe_float(row.get("final_wss")), reverse=True)
    return rows


def manual_candidates_for_category(category: str, *, limit: int = 10) -> list[dict[str, str]]:
    review = load_pending_review()
    if not review:
        raise RuntimeError("no pending rebalance review")
    return _candidate_rows_for_category(review, category)[:limit]


def manual_candidate_categories() -> list[str]:
    review = load_pending_review()
    if not review:
        raise RuntimeError("no pending rebalance review")
    all_csv = Path(review["files"]["all_csv"])
    best_wss_by_category: dict[str, float] = {}
    for row in _read_csv(all_csv):
        if str(row.get("eligible") or "").lower() != "true":
            continue
        category = str(row.get("category") or "").upper()
        if not category:
            continue
        best_wss_by_category[category] = max(
            best_wss_by_category.get(category, float("-inf")),
            _safe_float(row.get("final_wss")),
        )
    return [
        category
        for category, _score in sorted(
            best_wss_by_category.items(),
            key=lambda item: (item[1], item[0]),
            reverse=True,
        )
    ]


def list_manual_candidates(category: str, *, limit: int = 10) -> str:
    review = load_pending_review()
    if not review:
        return "Нет pending rebalance review. Сначала отправь Ребаланс."
    rows = _candidate_rows_for_category(review, category)
    if not rows:
        return f"Нет eligible кандидатов для {category}."
    lines = [f"Кандидаты {category.upper()}:"]
    for idx, row in enumerate(rows[:limit], start=1):
        lines.append(
            f"{idx}. {row.get('user_name')} | WSS {row.get('final_wss')} | "
            f"copy {row.get('copyability_score')} | "
            f"rank {row.get('rank')} | last {row.get('days_since_last_trade')}d | "
            f"flow BUY/SELL {row.get('buy_trades_30d', '')}/{row.get('sell_trades_30d', '')} | "
            f"openPnL {row.get('current_position_pnl_ratio')}"
        )
    lines.append("")
    lines.append(f"Чтобы выбрать: pick {category.upper()} 1")
    return "\n".join(lines)


def apply_manual_pick(
    category: str,
    pick_index: int,
    *,
    review_id: str | None = None,
) -> dict[str, Any]:
    review = load_pending_review()
    if not review:
        raise RuntimeError("no pending rebalance review")
    if review_id and review.get("review_id") != review_id:
        raise RuntimeError(f"pending review id mismatch: {review.get('review_id')} != {review_id}")

    candidates = _candidate_rows_for_category(review, category)
    if pick_index < 1 or pick_index > len(candidates):
        raise RuntimeError(f"pick index out of range for {category}: {pick_index}")
    chosen = candidates[pick_index - 1]

    live_path = Path(review["files"]["live"])
    live_rows = _read_csv(live_path)
    if not live_rows:
        raise RuntimeError("pending live universe is empty")

    category_upper = category.upper()
    replace_idx = next(
        (idx for idx, row in enumerate(live_rows) if str(row.get("category") or "").upper() == category_upper),
        None,
    )
    replaced_category = None
    if replace_idx is None:
        replace_idx = min(range(len(live_rows)), key=lambda idx: _safe_float(live_rows[idx].get("final_wss")))
        replaced_category = live_rows[replace_idx].get("category")

    live_row = dict(chosen)
    live_row["all_categories"] = chosen.get("all_categories") or chosen.get("category")
    live_rows[replace_idx] = live_row
    live_rows = _reweight_live_rows(live_rows)
    _write_csv(live_rows, live_path, _live_fieldnames(live_rows))
    _write_csv(
        [
            {
                "category": row.get("category"),
                "decision": "MANUAL_REVIEW_SELECTION",
                "selected_wallet": row.get("wallet"),
                "selected_user_name": row.get("user_name"),
                "selected_wss": row.get("final_wss"),
                "selected_weight": row.get("weight"),
                "reason": "manual pick in Telegram pending review",
            }
            for row in live_rows
        ],
        Path(review["files"]["report"]),
        [
            "category",
            "decision",
            "selected_wallet",
            "selected_user_name",
            "selected_wss",
            "selected_weight",
            "reason",
        ],
    )

    review["manual_overrides"][category_upper] = {
        "user_name": chosen.get("user_name"),
        "wallet": chosen.get("wallet"),
        "pick_index": pick_index,
        "replaced_category": replaced_category,
    }
    review["proposed_live"] = live_rows
    _set_pending_review(review)

    return {
        "review": review,
        "chosen": chosen,
        "replaced_category": replaced_category,
    }


def apply_manual_replacement(
    *,
    replace_index: int,
    candidate_category: str,
    pick_index: int,
    review_id: str | None = None,
) -> dict[str, Any]:
    review = load_pending_review()
    if not review:
        raise RuntimeError("no pending rebalance review")
    if review_id and review.get("review_id") != review_id:
        raise RuntimeError(f"pending review id mismatch: {review.get('review_id')} != {review_id}")

    candidates = _candidate_rows_for_category(review, candidate_category)
    if pick_index < 1 or pick_index > len(candidates):
        raise RuntimeError(f"pick index out of range for {candidate_category}: {pick_index}")
    chosen = candidates[pick_index - 1]

    live_path = Path(review["files"]["live"])
    live_rows = _read_csv(live_path)
    if not live_rows:
        raise RuntimeError("pending live universe is empty")
    if replace_index < 1 or replace_index > len(live_rows):
        raise RuntimeError(f"replace index out of range: {replace_index}")

    replace_idx = replace_index - 1
    chosen_wallet = str(chosen.get("wallet") or "").lower()
    duplicate = next(
        (
            row
            for idx, row in enumerate(live_rows)
            if idx != replace_idx and str(row.get("wallet") or "").lower() == chosen_wallet
        ),
        None,
    )
    if duplicate is not None:
        raise RuntimeError(
            "chosen wallet is already in proposed live universe: "
            f"{duplicate.get('user_name')} | {duplicate.get('category')}"
        )

    replaced = dict(live_rows[replace_idx])
    live_row = dict(chosen)
    live_row["all_categories"] = chosen.get("all_categories") or chosen.get("category")
    live_rows[replace_idx] = live_row
    live_rows = _reweight_live_rows(live_rows)
    _write_csv(live_rows, live_path, _live_fieldnames(live_rows))
    _write_csv(
        [
            {
                "category": row.get("category"),
                "decision": "MANUAL_REVIEW_REPLACEMENT",
                "selected_wallet": row.get("wallet"),
                "selected_user_name": row.get("user_name"),
                "selected_wss": row.get("final_wss"),
                "selected_weight": row.get("weight"),
                "reason": "manual replacement in Telegram pending review",
            }
            for row in live_rows
        ],
        Path(review["files"]["report"]),
        [
            "category",
            "decision",
            "selected_wallet",
            "selected_user_name",
            "selected_wss",
            "selected_weight",
            "reason",
        ],
    )

    review["manual_overrides"][f"slot_{replace_index}"] = {
        "replaced_user_name": replaced.get("user_name"),
        "replaced_wallet": replaced.get("wallet"),
        "replaced_category": replaced.get("category"),
        "selected_user_name": chosen.get("user_name"),
        "selected_wallet": chosen.get("wallet"),
        "selected_category": chosen.get("category"),
        "pick_index": pick_index,
    }
    review["proposed_live"] = live_rows
    _set_pending_review(review)

    return {
        "review": review,
        "chosen": chosen,
        "replaced": replaced,
        "replace_index": replace_index,
    }


def approve_pending_review(review_id: str | None = None) -> str:
    review = load_pending_review()
    if not review:
        raise RuntimeError("no pending rebalance review")
    if review_id and review.get("review_id") != review_id:
        raise RuntimeError(f"pending review id mismatch: {review.get('review_id')} != {review_id}")

    files = review["files"]
    all_csv = files.get("all_csv")
    if all_csv:
        _validate_review_rows(_read_csv(Path(all_csv)))
    _copy_required(Path(files["final_candidates"]), FINAL_CANDIDATES_FILE)
    _copy_required(Path(files["final_allocation"]), FINAL_ALLOCATION_FILE)
    _copy_required(Path(files["live"]), LIVE_FILE)
    _copy_required(Path(files["report"]), REPORT_FILE)
    _copy_required(Path(files["state"]), STATE_FILE)

    buf = StringIO()
    with redirect_stdout(buf):
        apply_rebalance_lifecycle()

    review["status"] = "APPROVED"
    review["approved_at"] = datetime.now(timezone.utc).isoformat()
    _set_pending_review(review)
    return buf.getvalue()


def reject_pending_review(review_id: str | None = None) -> str:
    review = load_pending_review()
    if not review:
        raise RuntimeError("no pending rebalance review")
    if review_id and review.get("review_id") != review_id:
        raise RuntimeError(f"pending review id mismatch: {review.get('review_id')} != {review_id}")
    review["status"] = "REJECTED"
    review["rejected_at"] = datetime.now(timezone.utc).isoformat()
    _set_pending_review(review)
    return f"Rebalance review {review['review_id']} rejected. Live universe не изменен."


def main() -> None:
    review = create_rebalance_review()
    print(build_review_message(review))
    print(f"CSV: {review['files']['all_csv']}")
    print(f"XLSX: {review['files']['xlsx']}")


if __name__ == "__main__":
    main()
