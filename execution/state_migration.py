from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from execution.state_store import (
    create_order_attempt,
    list_order_attempts,
    list_processed_signals,
    update_order_attempt,
)


LEGACY_PREVIEW_STATUSES = {
    "PREVIEW_READY",
    "PREVIEW_READY_ENTRY",
    "PREVIEW_READY_EXIT",
    "PREVIEW_READY_PARTIAL_EXIT",
}

LEGACY_PAPER_STATUSES = {
    "PAPER_FILLED_ENTRY",
    "PAPER_FILLED_EXIT",
    "PAPER_FILLED_PARTIAL_EXIT",
}


@dataclass(frozen=True)
class LegacyAttemptBackfill:
    signal_id: str
    leader_wallet: str
    token_id: str
    side: str
    amount_usd: float
    mode: str
    attempt_status: str
    source_status: str


def _safe_float(value: Any) -> float:
    try:
        return float(value) if value is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def plan_legacy_order_attempt_backfill(
    *,
    processed_signal_rows: list[dict[str, Any]],
    order_attempt_rows: list[dict[str, Any]],
) -> list[LegacyAttemptBackfill]:
    attempted_signal_ids = {row["signal_id"] for row in order_attempt_rows}
    planned: list[LegacyAttemptBackfill] = []

    for row in processed_signal_rows:
        signal_id = row.get("signal_id")
        status = str(row.get("status") or "")
        if not signal_id or signal_id in attempted_signal_ids:
            continue

        if status in LEGACY_PREVIEW_STATUSES:
            mode = "PREVIEW"
            attempt_status = "PREVIEW_READY"
        elif status in LEGACY_PAPER_STATUSES:
            mode = "PAPER"
            attempt_status = "PAPER_FILLED"
        else:
            continue

        amount_usd = _safe_float(row.get("suggested_amount_usd"))
        if amount_usd <= 0:
            continue

        planned.append(
            LegacyAttemptBackfill(
                signal_id=str(signal_id),
                leader_wallet=str(row.get("leader_wallet") or ""),
                token_id=str(row.get("token_id") or ""),
                side=str(row.get("side") or ""),
                amount_usd=amount_usd,
                mode=mode,
                attempt_status=attempt_status,
                source_status=status,
            )
        )

    return planned


def apply_legacy_order_attempt_backfill() -> list[dict[str, Any]]:
    planned = plan_legacy_order_attempt_backfill(
        processed_signal_rows=list_processed_signals(limit=100000),
        order_attempt_rows=list_order_attempts(limit=100000),
    )

    applied: list[dict[str, Any]] = []
    for item in planned:
        attempt_id = create_order_attempt(
            signal_id=item.signal_id,
            leader_wallet=item.leader_wallet,
            token_id=item.token_id,
            side=item.side,
            amount_usd=item.amount_usd,
            mode=item.mode,
            status="BACKFILLING",
            reason=f"legacy order_attempt backfill from {item.source_status}",
        )
        update_order_attempt(
            attempt_id=attempt_id,
            status=item.attempt_status,
            reason=f"legacy order_attempt backfill from {item.source_status}",
            raw_response={
                "legacy_backfill": True,
                "source_status": item.source_status,
            },
            fill_amount_usd=item.amount_usd,
        )
        applied.append(
            {
                "attempt_id": attempt_id,
                "signal_id": item.signal_id,
                "mode": item.mode,
                "status": item.attempt_status,
                "amount_usd": item.amount_usd,
                "source_status": item.source_status,
            }
        )

    return applied
