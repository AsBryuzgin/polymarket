from __future__ import annotations

import time
from typing import Callable


def remaining_cycle_sleep_sec(
    *,
    cycle_started_monotonic: float,
    interval_sec: float,
    now_monotonic: float | None = None,
) -> float:
    now_monotonic = time.monotonic() if now_monotonic is None else now_monotonic
    elapsed = max(now_monotonic - cycle_started_monotonic, 0.0)
    return max(float(interval_sec) - elapsed, 0.0)


def sleep_until_next_cycle(
    *,
    cycle_started_monotonic: float,
    interval_sec: float,
    sleeper: Callable[[float], None] = time.sleep,
) -> float:
    sleep_sec = remaining_cycle_sleep_sec(
        cycle_started_monotonic=cycle_started_monotonic,
        interval_sec=interval_sec,
    )
    if sleep_sec > 0:
        sleeper(sleep_sec)
    return sleep_sec
