from __future__ import annotations

from dataclasses import dataclass
from math import log1p
from statistics import median, pstdev
from typing import List


def clip01(x: float) -> float:
    return max(0.0, min(1.0, x))


def safe_std(values: List[float]) -> float:
    clean = [float(v) for v in values if v is not None]
    if len(clean) <= 1:
        return 0.0
    return float(pstdev(clean))


def safe_median(values: List[float]) -> float:
    clean = [float(v) for v in values if v is not None]
    if not clean:
        return 0.0
    return float(median(clean))


@dataclass
class WalletMetrics:
    age_days: int
    closed_positions: int
    unique_markets: int
    primary_domain_share: float
    single_market_concentration: float
    roi_30: float
    roi_90: float
    roi_180: float
    monthly_roi_last_6: List[float]
    negative_monthly_roi_last_12: List[float]
    primary_domain_roi_30: float
    primary_domain_roi_90: float
    primary_domain_roi_180: float
    max_drawdown: float
    longest_loss_streak: int
    median_spread: float
    median_liquidity: float
    slippage_proxy: float
    delay_sec: float
    profit_factor: float
    largest_win_share: float
    trades_30d: int = 0
    trades_90d: int = 0
    days_since_last_trade: int = 9999


@dataclass
class WalletScoreBreakdown:
    eligible: bool
    filter_reasons: List[str]
    consistency_score: float
    drawdown_score: float
    specialization_score: float
    copyability_score: float
    activity_score: float
    return_quality_score: float
    raw_wss: float
    track_record_multiplier: float
    data_depth_multiplier: float
    final_wss: float


def check_wallet_filters(metrics: WalletMetrics) -> tuple[bool, List[str]]:
    reasons: List[str] = []

    if metrics.age_days < 120:
        reasons.append("age_days < 120")
    if metrics.closed_positions < 40:
        reasons.append("closed_positions < 40")
    if metrics.unique_markets < 15:
        reasons.append("unique_markets < 15")
    if metrics.primary_domain_share < 0.35:
        reasons.append("primary_domain_share < 0.35")
    if metrics.single_market_concentration > 0.35:
        reasons.append("single_market_concentration > 0.35")
    if metrics.trades_90d < 3:
        reasons.append("trades_90d < 3")
    if metrics.days_since_last_trade > 45:
        reasons.append("days_since_last_trade > 45")

    return len(reasons) == 0, reasons


def consistency_score(metrics: WalletMetrics) -> float:
    pwr = (
        0.2 * float(metrics.roi_30 > 0)
        + 0.3 * float(metrics.roi_90 > 0)
        + 0.5 * float(metrics.roi_180 > 0)
    )
    rs = 1.0 - clip01(
        safe_std([metrics.roi_30, metrics.roi_90, metrics.roi_180]) / 0.15
    )
    mc = clip01((safe_median(metrics.monthly_roi_last_6) + 0.02) / 0.07)
    return 100.0 * (0.50 * pwr + 0.30 * rs + 0.20 * mc)


def drawdown_control_score(metrics: WalletMetrics) -> float:
    mdd_score = 1.0 - clip01(metrics.max_drawdown / 0.30)
    ls_score = 1.0 - clip01(metrics.longest_loss_streak / 6.0)
    dv_std = safe_std(metrics.negative_monthly_roi_last_12)
    dv_score = 1.0 - clip01(dv_std / 0.10)
    return 100.0 * (0.50 * mdd_score + 0.30 * ls_score + 0.20 * dv_score)


def specialization_score(metrics: WalletMetrics) -> float:
    dfb = 1.0 - clip01(abs(metrics.primary_domain_share - 0.60) / 0.40)
    pdc = (
        0.2 * float(metrics.primary_domain_roi_30 > 0)
        + 0.3 * float(metrics.primary_domain_roi_90 > 0)
        + 0.5 * float(metrics.primary_domain_roi_180 > 0)
    )
    smc = 1.0 - clip01((metrics.single_market_concentration - 0.25) / 0.25)
    return 100.0 * (0.40 * dfb + 0.40 * pdc + 0.20 * smc)


def copyability_score(metrics: WalletMetrics) -> float:
    spread_score = 1.0 - clip01(metrics.median_spread / 0.03)
    liquidity_score = clip01(log1p(metrics.median_liquidity) / log1p(50000.0))
    slippage_score = 1.0 - clip01(metrics.slippage_proxy / 0.02)
    delay_score = 1.0 - clip01(metrics.delay_sec / 300.0)

    return 100.0 * (
        0.35 * spread_score
        + 0.35 * liquidity_score
        + 0.20 * slippage_score
        + 0.10 * delay_score
    )


def activity_score(metrics: WalletMetrics) -> float:
    trades_30_score = clip01(metrics.trades_30d / 8.0)
    trades_90_score = clip01(metrics.trades_90d / 20.0)
    recency_score = 1.0 - clip01(max(metrics.days_since_last_trade - 7, 0) / 38.0)
    return 100.0 * (
        0.50 * trades_30_score
        + 0.30 * trades_90_score
        + 0.20 * recency_score
    )


def return_quality_score(metrics: WalletMetrics) -> float:
    roi_180_score = clip01(metrics.roi_180 / 0.25)
    pf_score = clip01(metrics.profit_factor / 2.5)
    lw_score = 1.0 - clip01(metrics.largest_win_share / 0.40)
    return 100.0 * (0.40 * roi_180_score + 0.30 * pf_score + 0.30 * lw_score)


def track_record_multiplier(metrics: WalletMetrics) -> float:
    return 0.5 + 0.5 * clip01(metrics.age_days / 365.0)


def data_depth_multiplier(metrics: WalletMetrics) -> float:
    return (
        0.5
        + 0.25 * clip01(metrics.closed_positions / 100.0)
        + 0.25 * clip01(metrics.unique_markets / 30.0)
    )


def score_wallet(metrics: WalletMetrics) -> WalletScoreBreakdown:
    eligible, reasons = check_wallet_filters(metrics)

    c = consistency_score(metrics)
    d = drawdown_control_score(metrics)
    s = specialization_score(metrics)
    k = copyability_score(metrics)
    a = activity_score(metrics)
    r = return_quality_score(metrics)

    raw_wss = (
        0.30 * c
        + 0.25 * d
        + 0.20 * s
        + 0.10 * k
        + 0.05 * a
        + 0.10 * r
    )

    tr = track_record_multiplier(metrics)
    dd = data_depth_multiplier(metrics)
    final_wss = raw_wss * tr * dd

    return WalletScoreBreakdown(
        eligible=eligible,
        filter_reasons=reasons,
        consistency_score=round(c, 2),
        drawdown_score=round(d, 2),
        specialization_score=round(s, 2),
        copyability_score=round(k, 2),
        activity_score=round(a, 2),
        return_quality_score=round(r, 2),
        raw_wss=round(raw_wss, 2),
        track_record_multiplier=round(tr, 4),
        data_depth_multiplier=round(dd, 4),
        final_wss=round(final_wss, 2),
    )
