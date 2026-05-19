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
from app.allocation_runtime import resolve_leader_budget_usd, resolve_total_capital_usd
from execution.builder_auth import load_executor_config
from signals.economic_copyability import (
    ECONOMIC_COPYABILITY_REQUIREMENT_SAMPLES_FIELD,
    annotate_rows_with_economic_copyability,
    compute_budget_volume_coverage_by_wallet,
    requirement_samples_volume_coverage,
)


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
SCORING_VERSION = "wss_v4_profile_pnl_gates_2026_04_29"

REVIEW_COLUMNS = [
    "category",
    "time_period",
    "rank",
    "user_name",
    "wallet",
    "leaderboard_pnl",
    "leaderboard_week_pnl",
    "leaderboard_month_pnl",
    "profile_week_pnl",
    "profile_month_pnl",
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
    "total_pnl_ratio",
    "open_loss_exposure",
    "roi_7",
    "roi_30",
    "trades_30d",
    "trades_90d",
    "buy_trades_30d",
    "sell_trades_30d",
    "buy_trade_share_30d",
    "economic_copyability_status",
    "economic_copyability_source",
    "economic_copyability_reason",
    "economic_copyability_buy_signals",
    "economic_copyability_executable_ratio",
    "economic_copyability_batchable_ratio",
    "economic_copyability_dust_ratio",
    "economic_copyability_trade_fraction_samples",
    "economic_copyability_median_trade_fraction",
    "economic_copyability_mean_trade_fraction",
    "economic_copyability_median_copy_amount_usd",
    "economic_copyability_required_bankroll_p95_signals_usd",
    "economic_copyability_required_bankroll_p99_signals_usd",
    "economic_copyability_required_bankroll_p95_batch_usd",
    "economic_copyability_required_bankroll_p99_batch_usd",
    "economic_copyability_required_bankroll_p95_volume_usd",
    "economic_copyability_required_bankroll_p99_volume_usd",
    "economic_copyability_budget_usd",
    "economic_copyability_volume_coverage",
    "economic_copyability_volume_coverage_with_roundup",
    "economic_copyability_executable_now",
    "economic_copyability_executable_with_roundup",
    "economic_copyability_executable_after_batch",
    "economic_copyability_dust_signals",
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
    "total_pnl_ratio",
    "open_loss_exposure",
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
    "profile_week_pnl",
    "profile_month_pnl",
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


def _is_eligible(row: dict[str, Any]) -> bool:
    return str(row.get("eligible") or "").strip().lower() == "true"


def _short_text(value: Any, *, limit: int = 120) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _manual_selection_reason(row: dict[str, Any], base_reason: str) -> str:
    if "eligible" not in row or _is_eligible(row):
        return base_reason
    filter_reason = _short_text(row.get("filter_reasons"), limit=160)
    if filter_reason:
        return f"{base_reason}; manual ineligible override: {filter_reason}"
    return f"{base_reason}; manual ineligible override"


def format_manual_candidate_line(index: int, row: dict[str, Any]) -> str:
    econ = ""
    has_econ = any(
        str(row.get(key) or "").strip()
        for key in (
            "economic_copyability_median_trade_fraction",
            "economic_copyability_required_bankroll_p95_volume_usd",
            "economic_copyability_volume_coverage",
        )
    )
    if has_econ:
        req95 = _safe_float(row.get("economic_copyability_required_bankroll_p95_volume_usd"))
        req99 = _safe_float(row.get("economic_copyability_required_bankroll_p99_volume_usd"))
        req = ""
        if req95 > 0:
            req = f" | req vol95 ${req95:.0f}"
            if req99 > 0:
                req += f" / vol99 ${req99:.0f}"
        coverage = ""
        if str(row.get("economic_copyability_volume_coverage") or "").strip():
            coverage = (
                " | current vol "
                f"{_safe_float(row.get('economic_copyability_volume_coverage')):.0%}"
                "/"
                f"{_safe_float(row.get('economic_copyability_volume_coverage_with_roundup')):.0%} round"
            )
        fraction = ""
        if str(row.get("economic_copyability_median_trade_fraction") or "").strip():
            fraction = (
                " | econ med/avg "
                f"{_safe_float(row.get('economic_copyability_median_trade_fraction')):.2%}/"
                f"{_safe_float(row.get('economic_copyability_mean_trade_fraction')):.2%}"
            )
        econ = f"{fraction}{req}{coverage}"
    line = (
        f"{index}. {row.get('user_name')} | WSS {row.get('final_wss')} | "
        f"copy {row.get('copyability_score')} | "
        f"rank {row.get('rank')} | last {row.get('days_since_last_trade')}d | "
        f"profile 1W/1M {row.get('profile_week_pnl')}/{row.get('profile_month_pnl')} | "
        f"flow BUY/SELL {row.get('buy_trades_30d', '')}/{row.get('sell_trades_30d', '')} | "
        f"openPnL {row.get('current_position_pnl_ratio')} | "
        f"totalPnL {row.get('total_pnl_ratio')}"
        f"{econ}"
    )
    if not _is_eligible(row):
        reason = _short_text(row.get("filter_reasons"), limit=120)
        line += "\n   eligible=false"
        if reason:
            line += f"\n   причина: {reason}"
    return line


def format_manual_candidate_button_label(index: int, row: dict[str, Any]) -> str:
    suffix = "" if _is_eligible(row) else " | ineligible"
    req95 = _safe_float(row.get("economic_copyability_required_bankroll_p95_volume_usd"))
    req = f" | req ${req95:.0f}" if req95 > 0 else ""
    return (
        f"{index}. {row.get('user_name')} | WSS {row.get('final_wss')} | "
        f"copy {row.get('copyability_score')}{req}{suffix}"
    )


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
        ["consistency_score", 0.35, "realized ROI signs/stability plus total PnL including current open positions"],
        ["drawdown_score", 0.25, "closed-position drawdown proxy plus current open PnL and losing open exposure"],
        ["specialization_score", 0.20, "domain focus and single-market concentration penalty"],
        ["copyability_score", 0.10, "spread, liquidity, slippage proxy, delay"],
        ["return_quality_score", 0.10, "ROI 180, total PnL, capped profit factor, largest-win dependency"],
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
            "leader must have positive profile week and month PnL when profile PnL data is available; otherwise falls back to leaderboard PnL or closed-position ROI",
        ],
        [
            "copy-flow filter",
            "display/gate only; not included in WSS",
            "rejects SELL-only or near SELL-only recent taker flow that cannot open copy entries",
        ],
        [
            "economic copyability",
            "historical/runtime gate only; not included in WSS",
            "uses recent wallet trades for new candidates and runtime signal observations when available; rejects leaders whose BUY flow is mostly too small for the current bankroll/min-order model even after short batching",
        ],
        [
            "hard gates",
            "age>=120, closed>=40, unique>=15, notional concentration<=35%, open_pnl>=-10%, trades30>=30, last_trade<=5d, copyability>=60, positive week/month PnL, copy-flow buy presence, economic copyability if runtime samples are sufficient",
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
    _apply_economic_copyability_annotations(load_executor_config())
    rows = _all_review_rows()
    _validate_review_rows(rows)
    return buf.getvalue()


def _append_fieldnames(existing: list[str], rows: list[dict[str, Any]]) -> list[str]:
    fieldnames = list(existing)
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    return fieldnames


def _apply_economic_copyability_annotations(config: dict[str, Any]) -> None:
    for path in (MASTER_CORE_FILE, MASTER_EXPERIMENTAL_FILE):
        rows = _read_csv(path)
        if not rows:
            continue
        original_fieldnames = list(rows[0].keys())
        annotate_rows_with_economic_copyability(rows, config=config)
        _write_csv(rows, path, _append_fieldnames(original_fieldnames, rows))


def _review_total_capital_usd(config: dict[str, Any]) -> float:
    try:
        return resolve_total_capital_usd(
            executor_config=config,
            default=0.0,
            allow_zero_collateral_balance=True,
        )
    except Exception:
        return _safe_float(config.get("capital", {}).get("total_capital_usd"))


def _annotate_budget_volume_coverage(
    rows: list[dict[str, Any]],
    *,
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    if not rows:
        return rows
    total_capital_usd = _review_total_capital_usd(config)
    budget_by_wallet: dict[str, float] = {}
    for row in rows:
        wallet = str(row.get("wallet") or "").lower()
        if not wallet:
            continue
        budget_by_wallet[wallet] = resolve_leader_budget_usd(
            row,
            total_capital_usd=total_capital_usd,
        )

    coverage_by_wallet = compute_budget_volume_coverage_by_wallet(
        config=config,
        budget_by_wallet=budget_by_wallet,
    )
    sizing_cfg = config.get("sizing", {})
    max_round_up_multiple = _safe_float(
        sizing_cfg.get("max_min_order_round_up_multiple"),
        3.0,
    )
    for row in rows:
        wallet = str(row.get("wallet") or "").lower()
        budget = budget_by_wallet.get(wallet)
        if budget is not None:
            row["economic_copyability_budget_usd"] = round(budget, 2)
        coverage = coverage_by_wallet.get(wallet)
        if not coverage and budget is not None:
            coverage = requirement_samples_volume_coverage(
                row.get(ECONOMIC_COPYABILITY_REQUIREMENT_SAMPLES_FIELD),
                budget_usd=budget,
                max_round_up_multiple=max_round_up_multiple,
            )
        if not coverage:
            continue
        row["economic_copyability_volume_coverage"] = coverage["volume_coverage"]
        row["economic_copyability_volume_coverage_with_roundup"] = coverage[
            "volume_coverage_with_roundup"
        ]
    return rows


def _capital_required_p95_volume(row: dict[str, Any]) -> float:
    return _safe_float(row.get("economic_copyability_required_bankroll_p95_volume_usd"))


def _capital_copy_rank(row: dict[str, Any], *, total_capital_usd: float) -> tuple[float, float, float, float, float]:
    required = _capital_required_p95_volume(row)
    affordability = min(total_capital_usd / required, 1.0) if required > 0 else 0.0
    status = str(row.get("economic_copyability_status") or "").upper()
    status_score = {"PASS": 1.0, "UNKNOWN": 0.35, "FAIL": -1.0}.get(status, 0.0)
    wss = _safe_float(row.get("final_wss")) / 100.0
    batchable = _safe_float(row.get("economic_copyability_batchable_ratio"))
    executable = _safe_float(row.get("economic_copyability_executable_ratio"))
    dust = _safe_float(row.get("economic_copyability_dust_ratio"))
    score = (
        0.36 * wss
        + 0.24 * batchable
        + 0.16 * executable
        + 0.14 * affordability
        + 0.10 * status_score
        - 0.12 * dust
    )
    return (
        score,
        status_score,
        batchable,
        executable,
        affordability,
    )


def _capital_capacity_summary(
    rows: list[dict[str, Any]],
    *,
    total_capital_usd: float,
) -> dict[str, Any]:
    budgets = [max(resolve_leader_budget_usd(row, total_capital_usd=total_capital_usd), 0.0) for row in rows]
    budget_total = sum(budgets)
    known_pairs = [
        (row, budget)
        for row, budget in zip(rows, budgets)
        if str(row.get("economic_copyability_status") or "").upper() != "UNKNOWN"
        and _safe_float(row.get("economic_copyability_buy_signals")) > 0
    ]
    known_budget_total = sum(budget for _row, budget in known_pairs)

    def weighted_average(key: str) -> float | None:
        pairs: list[tuple[float, float]] = []
        for row, budget in known_pairs:
            raw = row.get(key)
            if raw in (None, "", "n/a", "N/A"):
                continue
            pairs.append((budget, max(_safe_float(raw), 0.0)))
        denominator = sum(budget for budget, _value in pairs)
        if denominator <= 0:
            return None
        return sum(budget * value for budget, value in pairs) / denominator

    unknown = sum(
        1
        for row in rows
        if str(row.get("economic_copyability_status") or "").upper() == "UNKNOWN"
    )
    failures = sum(
        1
        for row in rows
        if str(row.get("economic_copyability_status") or "").upper() == "FAIL"
    )
    volume_coverage = weighted_average("economic_copyability_volume_coverage")
    volume_coverage_round = weighted_average("economic_copyability_volume_coverage_with_roundup")
    return {
        "leader_count": len(rows),
        "total_capital_usd": round(total_capital_usd, 2),
        "allocated_budget_usd": round(budget_total, 2),
        "known_leaders": len(known_pairs),
        "known_budget_usd": round(known_budget_total, 2),
        "executable_ratio": (
            round(value, 6)
            if (value := weighted_average("economic_copyability_executable_ratio")) is not None
            else None
        ),
        "batchable_ratio": (
            round(value, 6)
            if (value := weighted_average("economic_copyability_batchable_ratio")) is not None
            else None
        ),
        "dust_ratio": (
            round(value, 6)
            if (value := weighted_average("economic_copyability_dust_ratio")) is not None
            else None
        ),
        "volume_coverage": round(volume_coverage, 6) if volume_coverage is not None else None,
        "volume_coverage_with_roundup": (
            round(volume_coverage_round, 6) if volume_coverage_round is not None else None
        ),
        "estimated_idle_ratio": round(
            max(0.0, 1.0 - volume_coverage_round),
            6,
        )
        if volume_coverage_round is not None
        else None,
        "unknown_leaders": unknown,
        "failed_leaders": failures,
    }


def _pct_or_na(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{_safe_float(value):.0%}"


def _capital_summary_text(summary: dict[str, Any] | None) -> str:
    if not summary:
        return ""
    unknown_note = ""
    if _safe_int(summary.get("known_leaders")) == 0:
        unknown_note = "\nважно: по выбранным лидерам нет runtime-истории, это WSS-only выбор"
    elif _safe_int(summary.get("unknown_leaders")) > 0:
        unknown_note = (
            "\nважно: часть лидеров без runtime-истории, проценты считаются только по известным"
        )
    return (
        "Ожидаемая копируемость при текущем банкролле:\n"
        f"leaders: {summary.get('leader_count')} | "
        f"known: {summary.get('known_leaders')} | "
        f"allocated: ${_safe_float(summary.get('allocated_budget_usd')):.0f} / "
        f"${_safe_float(summary.get('total_capital_usd')):.0f}\n"
        f">= min сейчас: {_pct_or_na(summary.get('executable_ratio'))} | "
        f"после short batch: {_pct_or_na(summary.get('batchable_ratio'))}\n"
        f"volume coverage: {_pct_or_na(summary.get('volume_coverage'))} | "
        f"с round-up: {_pct_or_na(summary.get('volume_coverage_with_roundup'))}\n"
        f"примерно простаивает/dust: {_pct_or_na(summary.get('estimated_idle_ratio'))}"
        f"{unknown_note}"
    )


def _strict_capital_fit_note(
    rows: list[dict[str, Any]],
    *,
    total_capital_usd: float,
) -> str | None:
    if not rows or total_capital_usd <= 0:
        return None
    for row in rows:
        required = _capital_required_p95_volume(row)
        if required <= 0:
            return (
                f"{row.get('user_name')} has no runtime economic-copyability history; "
                "capital-aware review treats it as unproven"
            )
        budget = resolve_leader_budget_usd(row, total_capital_usd=total_capital_usd)
        if budget + 1e-12 < required:
            return (
                f"{row.get('user_name')} needs about ${required:.0f} per-leader bankroll "
                f"for p95 volume, but proposed budget is ${budget:.0f}"
            )
    return None


def _strict_capital_prune_live_rows(
    rows: list[dict[str, Any]],
    *,
    config: dict[str, Any],
    total_capital_usd: float,
) -> tuple[list[dict[str, Any]], str, dict[str, Any]]:
    cfg = config.get("economic_copyability", {})
    min_live_leaders = max(1, _safe_int(cfg.get("min_live_leaders"), 1))
    ranked = sorted(
        (dict(row) for row in rows),
        key=lambda row: _capital_copy_rank(row, total_capital_usd=total_capital_usd),
        reverse=True,
    )
    original_count = len(ranked)

    best_subset: list[dict[str, Any]] | None = None
    best_note = ""
    for size in range(original_count, min_live_leaders - 1, -1):
        subset = _annotate_budget_volume_coverage(
            _reweight_live_rows([dict(row) for row in ranked[:size]]),
            config=config,
        )
        note = _strict_capital_fit_note(subset, total_capital_usd=total_capital_usd)
        if note is None:
            best_subset = subset
            break
        best_note = note

    if best_subset is None:
        best_subset = _annotate_budget_volume_coverage(
            _reweight_live_rows([dict(ranked[0])]),
            config=config,
        )
        note = _strict_capital_fit_note(best_subset, total_capital_usd=total_capital_usd)
        if note:
            best_note = note

    summary = _capital_capacity_summary(best_subset, total_capital_usd=total_capital_usd)
    if len(best_subset) == original_count:
        return best_subset, "", summary

    return (
        best_subset,
        (
            "Capital-aware strict pruning: "
            f"${total_capital_usd:.0f} bankroll reduced proposed universe "
            f"from {original_count} to {len(best_subset)} leader(s). "
            f"{best_note}"
        ),
        summary,
    )


def _balanced_subset_score(
    rows: list[dict[str, Any]],
    *,
    total_capital_usd: float,
    target_live_leaders: int,
) -> tuple[float, dict[str, Any]]:
    summary = _capital_capacity_summary(rows, total_capital_usd=total_capital_usd)
    avg_wss = (
        sum(_safe_float(row.get("final_wss")) for row in rows) / len(rows)
        if rows
        else 0.0
    )
    count_score = 1.0 - min(
        abs(len(rows) - target_live_leaders) / max(target_live_leaders, 1),
        1.0,
    )
    score = (
        0.30 * (avg_wss / 100.0)
        + 0.25 * _safe_float(summary.get("volume_coverage_with_roundup"))
        + 0.18 * _safe_float(summary.get("batchable_ratio"))
        + 0.12 * _safe_float(summary.get("executable_ratio"))
        + 0.15 * count_score
        - 0.16 * _safe_float(summary.get("dust_ratio"))
        - 0.12 * _safe_float(summary.get("unknown_leaders")) / max(len(rows), 1)
        - 0.50 * _safe_float(summary.get("failed_leaders")) / max(len(rows), 1)
    )
    return score, summary


def _balanced_capital_select_live_rows(
    rows: list[dict[str, Any]],
    *,
    config: dict[str, Any],
    total_capital_usd: float,
) -> tuple[list[dict[str, Any]], str, dict[str, Any]]:
    cfg = config.get("economic_copyability", {})
    original_count = len(rows)
    min_live_leaders = max(1, _safe_int(cfg.get("min_live_leaders"), 2))
    target_live_leaders = max(
        min_live_leaders,
        _safe_int(cfg.get("target_live_leaders"), _safe_int(cfg.get("preferred_live_leaders"), 3)),
    )
    max_live_leaders = max(
        min_live_leaders,
        _safe_int(cfg.get("max_live_leaders"), target_live_leaders),
    )
    max_live_leaders = min(max_live_leaders, original_count)
    min_live_leaders = min(min_live_leaders, max_live_leaders)

    ranked = sorted(
        (dict(row) for row in rows),
        key=lambda row: _capital_copy_rank(row, total_capital_usd=total_capital_usd),
        reverse=True,
    )
    best_rows: list[dict[str, Any]] | None = None
    best_summary: dict[str, Any] = {}
    best_score = float("-inf")

    for size in range(min_live_leaders, max_live_leaders + 1):
        subset = _annotate_budget_volume_coverage(
            _reweight_live_rows([dict(row) for row in ranked[:size]]),
            config=config,
        )
        score, summary = _balanced_subset_score(
            subset,
            total_capital_usd=total_capital_usd,
            target_live_leaders=target_live_leaders,
        )
        if score > best_score:
            best_score = score
            best_rows = subset
            best_summary = summary

    assert best_rows is not None
    note_parts = [
        "Capital-aware balanced selection: "
        f"${total_capital_usd:.0f} bankroll chose {len(best_rows)} of {original_count} leader(s)"
    ]
    if len(best_rows) != original_count:
        note_parts.append("instead of requiring strict p95 fit for every leader")
    if _safe_int(best_summary.get("known_leaders")) == 0:
        note_parts.append(
            "runtime economic-copyability is unknown for all selected leaders; "
            "treat this as WSS-only until paper discovery collects signals"
        )
    else:
        note_parts.append(
            "volume coverage "
            f"{_pct_or_na(best_summary.get('volume_coverage'))}/"
            f"{_pct_or_na(best_summary.get('volume_coverage_with_roundup'))} with round-up"
        )
        note_parts.append(
            "batchable "
            f"{_pct_or_na(best_summary.get('batchable_ratio'))}; "
            "estimated idle/dust "
            f"{_pct_or_na(best_summary.get('estimated_idle_ratio'))}"
        )
    return best_rows, "; ".join(note_parts) + ".", best_summary


def _capital_prune_live_rows(
    rows: list[dict[str, Any]],
    *,
    config: dict[str, Any],
) -> tuple[list[dict[str, Any]], str, dict[str, Any]]:
    if not rows:
        return rows, "", {}

    cfg = config.get("economic_copyability", {})
    enabled = str(cfg.get("capital_aware_rebalance", "true")).strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }
    if not enabled:
        annotated = _annotate_budget_volume_coverage(rows, config=config)
        return annotated, "", {}

    total_capital_usd = _review_total_capital_usd(config)
    if total_capital_usd <= 0:
        annotated = _annotate_budget_volume_coverage(rows, config=config)
        return annotated, "", {}

    mode = str(cfg.get("capital_aware_rebalance_mode") or "balanced").strip().lower()
    if mode == "strict":
        return _strict_capital_prune_live_rows(
            rows,
            config=config,
            total_capital_usd=total_capital_usd,
        )
    return _balanced_capital_select_live_rows(
        rows,
        config=config,
        total_capital_usd=total_capital_usd,
    )


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
        eligibility = ""
        if "eligible" in row and not _is_eligible(row):
            eligibility = " | manual ineligible"
        copyability = ""
        budget = _safe_float(row.get("economic_copyability_budget_usd"))
        volume_coverage = _safe_float(row.get("economic_copyability_volume_coverage"))
        volume_coverage_round = _safe_float(
            row.get("economic_copyability_volume_coverage_with_roundup")
        )
        req95 = _safe_float(row.get("economic_copyability_required_bankroll_p95_volume_usd"))
        if budget > 0 and (volume_coverage > 0 or volume_coverage_round > 0):
            copyability = (
                f" | budget ${budget:.0f} | vol {volume_coverage:.0%}"
                f"/{volume_coverage_round:.0%} round"
            )
        elif req95 > 0:
            copyability = f" | req vol95 ${req95:.0f}"
        lines.append(
            f"{idx}. {row.get('user_name')} | {row.get('category')} | "
            f"WSS {row.get('final_wss')} | {weight:.2f}%{copyability}{eligibility}"
        )
    return "\n".join(lines)


def create_rebalance_review(*, refresh: bool = True) -> dict[str, Any]:
    review_id = _utc_review_id()
    paths = _pending_paths(review_id)
    paths["root"].mkdir(parents=True, exist_ok=True)

    refresh_log = ""
    if refresh:
        refresh_log = refresh_shortlists()
    else:
        _apply_economic_copyability_annotations(load_executor_config())

    all_rows = _all_review_rows()
    _validate_review_rows(all_rows)
    _write_csv(all_rows, paths["all_csv"], _append_fieldnames(REVIEW_COLUMNS, all_rows))
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
    config = load_executor_config()
    live_rows, capital_pruning_note, capital_fit_summary = _capital_prune_live_rows(
        live_rows,
        config=config,
    )
    _write_csv(live_rows, paths["live"], _live_fieldnames(live_rows))
    review = {
        "review_id": review_id,
        "scoring_version": SCORING_VERSION,
        "status": "PENDING",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "manual_overrides": {},
        "files": {key: str(path) for key, path in paths.items() if key != "root"},
        "proposed_live": live_rows,
        "capital_pruning_note": capital_pruning_note,
        "capital_fit_summary": capital_fit_summary,
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
    pruning = str(review.get("capital_pruning_note") or "").strip()
    pruning_text = f"\n\n{pruning}" if pruning else ""
    capital_summary = _capital_summary_text(review.get("capital_fit_summary"))
    capital_summary_text = f"\n\n{capital_summary}" if capital_summary else ""
    return (
        "Rebalance review готов\n"
        f"id: {review['review_id']}\n\n"
        "Предложенный live-universe:\n"
        f"{_summarize_live_rows(review.get('proposed_live') or [])}"
        f"{pruning_text}\n\n"
        f"{capital_summary_text}\n\n"
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
        "profile_week_pnl",
        "profile_month_pnl",
        "leaderboard_volume",
        "raw_weight",
        "weight",
        "trades_30d",
        "trades_90d",
        "buy_trades_30d",
        "sell_trades_30d",
        "buy_trade_share_30d",
        "economic_copyability_status",
        "economic_copyability_source",
        "economic_copyability_buy_signals",
        "economic_copyability_executable_ratio",
        "economic_copyability_batchable_ratio",
        "economic_copyability_dust_ratio",
        "economic_copyability_trade_fraction_samples",
        "economic_copyability_median_trade_fraction",
        "economic_copyability_mean_trade_fraction",
        "economic_copyability_required_bankroll_p95_signals_usd",
        "economic_copyability_required_bankroll_p99_signals_usd",
        "economic_copyability_required_bankroll_p95_batch_usd",
        "economic_copyability_required_bankroll_p99_batch_usd",
        "economic_copyability_required_bankroll_p95_volume_usd",
        "economic_copyability_required_bankroll_p99_volume_usd",
        "economic_copyability_budget_usd",
        "economic_copyability_volume_coverage",
        "economic_copyability_volume_coverage_with_roundup",
        "economic_copyability_requirement_samples_json",
        "economic_copyability_reason",
        "days_since_last_trade",
        "median_spread",
        "slippage_proxy",
        "current_position_pnl_ratio",
        "total_pnl_ratio",
        "open_loss_exposure",
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
        return f"Нет кандидатов для {category} в свежем top-30."
    lines = [f"Кандидаты {category.upper()}:"]
    for idx, row in enumerate(rows[:limit], start=1):
        lines.append(format_manual_candidate_line(idx, row))
    lines.append("")
    lines.append("Ручной выбор может взять eligible=false кандидата, но бот покажет причину фильтра.")
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
    config = load_executor_config()
    live_rows, capital_pruning_note, capital_fit_summary = _capital_prune_live_rows(
        live_rows,
        config=config,
    )
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
                "reason": _manual_selection_reason(row, "manual pick in Telegram pending review"),
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
        "eligible": chosen.get("eligible"),
        "filter_reasons": chosen.get("filter_reasons"),
    }
    review["proposed_live"] = live_rows
    review["capital_pruning_note"] = capital_pruning_note
    review["capital_fit_summary"] = capital_fit_summary
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
    config = load_executor_config()
    live_rows, capital_pruning_note, capital_fit_summary = _capital_prune_live_rows(
        live_rows,
        config=config,
    )
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
                "reason": _manual_selection_reason(row, "manual replacement in Telegram pending review"),
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
        "eligible": chosen.get("eligible"),
        "filter_reasons": chosen.get("filter_reasons"),
    }
    review["proposed_live"] = live_rows
    review["capital_pruning_note"] = capital_pruning_note
    review["capital_fit_summary"] = capital_fit_summary
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
