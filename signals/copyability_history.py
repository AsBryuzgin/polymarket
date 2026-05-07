from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any


DEFAULT_HISTORY_PATH = Path("data/shortlists/copyability_history.json")
MAX_SAMPLES_PER_WALLET = 12
SMOOTHING_SAMPLES = 5


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _load_history(path: Path = DEFAULT_HISTORY_PATH) -> dict[str, list[dict[str, Any]]]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _save_history(history: dict[str, list[dict[str, Any]]], path: Path = DEFAULT_HISTORY_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(history, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def record_copyability_score(
    *,
    wallet: str,
    category: str,
    score: float,
    path: Path = DEFAULT_HISTORY_PATH,
) -> tuple[float, int]:
    wallet_key = str(wallet or "").lower()
    raw_score = _safe_float(score)
    if not wallet_key or raw_score is None:
        return float(score), 1

    history = _load_history(path)
    samples = list(history.get(wallet_key) or [])
    samples.append(
        {
            "observed_at": _utc_now_iso(),
            "category": category,
            "copyability_score": round(max(0.0, min(100.0, raw_score)), 4),
        }
    )
    samples = samples[-MAX_SAMPLES_PER_WALLET:]
    history[wallet_key] = samples
    _save_history(history, path)

    recent_scores = [
        parsed
        for parsed in (_safe_float(row.get("copyability_score")) for row in samples[-SMOOTHING_SAMPLES:])
        if parsed is not None
    ]
    if not recent_scores:
        return raw_score, 1
    return round(float(median(recent_scores)), 2), len(recent_scores)

