from __future__ import annotations

from dataclasses import dataclass
from math import log1p
from statistics import median, pstdev
from typing import List


MIN_COPYABILITY_SCORE = 60.0
MIN_TRADES_30D = 30
MAX_DAYS_SINCE_LAST_TRADE = 5
MIN_COPY_FLOW_BUY_TRADES_30D = 3
MIN_COPY_FLOW_BUY_SHARE_30D = 0.10
MIN_COPY_FLOW_TRADES_FOR_SHARE_FILTER = 20


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
    roi_7: float = 0.0
    current_position_pnl_ratio: float = 0.0
    total_pnl_ratio: float = 0.0
    open_loss_exposure: float = 0.0
    trades_30d: int = 0
    trades_90d: int = 0
    buy_trades_30d: int = 0
    sell_trades_30d: int = 0
    buy_trade_share_30d: float = 0.0
    days_since_last_trade: int = 9999
    leaderboard_week_pnl: float | None = None
    leaderboard_month_pnl: float | None = None
    profile_week_pnl: float | None = None
    profile_month_pnl: float | None = None
    copyability_score_override: float | None = None


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


def check_wallet_filters(
    metrics: WalletMetrics,
    *,
    copyability: float | None = None,
) -> tuple[bool, List[str]]:
    reasons: List[str] = []

    if metrics.age_days < 120:
        reasons.append("age_days < 120")
    if metrics.closed_positions < 40:
        reasons.append("closed_positions < 40")
    if metrics.unique_markets < 15:
        reasons.append("unique_markets < 15")
    if metrics.single_market_concentration > 0.35:
        reasons.append("single_market_concentration > 0.35")
    if metrics.current_position_pnl_ratio < -0.10:
        reasons.append("current_position_pnl_ratio < -0.10")
    if metrics.trades_30d < MIN_TRADES_30D:
        reasons.append(f"trades_30d < {MIN_TRADES_30D:g}")
    if metrics.days_since_last_trade > MAX_DAYS_SINCE_LAST_TRADE:
        reasons.append(f"days_since_last_trade > {MAX_DAYS_SINCE_LAST_TRADE:g}")
    if copyability is not None and copyability < MIN_COPYABILITY_SCORE:
        reasons.append(f"copyability_score < {MIN_COPYABILITY_SCORE:g}")
    if metrics.profile_week_pnl is not None:
        if metrics.profile_week_pnl <= 0:
            reasons.append("profile_week_pnl <= 0")
    else:
        week_positive = (
            (metrics.leaderboard_week_pnl is not None and metrics.leaderboard_week_pnl > 0)
            or metrics.roi_7 > 0
        )
        if not week_positive:
            reasons.append("recent_week_pnl <= 0")

    if metrics.profile_month_pnl is not None:
        if metrics.profile_month_pnl <= 0:
            reasons.append("profile_month_pnl <= 0")
    else:
        month_positive = (
            (metrics.leaderboard_month_pnl is not None and metrics.leaderboard_month_pnl > 0)
            or metrics.roi_30 > 0
        )
        if not month_positive:
            reasons.append("recent_month_pnl <= 0")

    side_trades_30d = metrics.buy_trades_30d + metrics.sell_trades_30d
    if side_trades_30d > 0 and metrics.sell_trades_30d > 0:
        if metrics.buy_trades_30d < MIN_COPY_FLOW_BUY_TRADES_30D:
            reasons.append(f"copy_flow_buy_trades_30d < {MIN_COPY_FLOW_BUY_TRADES_30D:g}")
        elif (
            side_trades_30d >= MIN_COPY_FLOW_TRADES_FOR_SHARE_FILTER
            and metrics.buy_trade_share_30d < MIN_COPY_FLOW_BUY_SHARE_30D
        ):
            reasons.append(f"copy_flow_buy_share_30d < {MIN_COPY_FLOW_BUY_SHARE_30D:g}")

    return len(reasons) == 0, reasons


def consistency_score(metrics: WalletMetrics) -> float:
    realized_positive_windows = (
        0.1 * float(metrics.roi_7 > 0)
        + 0.2 * float(metrics.roi_30 > 0)
        + 0.3 * float(metrics.roi_90 > 0)
        + 0.4 * float(metrics.roi_180 > 0)
    )
    total_pnl_score = clip01((metrics.total_pnl_ratio + 0.02) / 0.14)
    rs = 1.0 - clip01(
        safe_std([metrics.roi_7, metrics.roi_30, metrics.roi_90, metrics.roi_180]) / 0.15
    )
    mc = clip01((safe_median(metrics.monthly_roi_last_6) + 0.02) / 0.07)
    return 100.0 * (
        0.40 * realized_positive_windows
        + 0.20 * total_pnl_score
        + 0.25 * rs
        + 0.15 * mc
    )


def drawdown_control_score(metrics: WalletMetrics) -> float:
    mdd_score = 1.0 - clip01(metrics.max_drawdown / 0.30)
    ls_score = 1.0 - clip01(metrics.longest_loss_streak / 6.0)
    dv_std = safe_std(metrics.negative_monthly_roi_last_12)
    dv_score = 1.0 - clip01(dv_std / 0.10)
    open_pnl_score = clip01((metrics.current_position_pnl_ratio + 0.10) / 0.20)
    open_loss_score = 1.0 - clip01(metrics.open_loss_exposure / 0.75)
    return 100.0 * (
        0.40 * mdd_score
        + 0.25 * ls_score
        + 0.15 * dv_score
        + 0.10 * open_pnl_score
        + 0.10 * open_loss_score
    )


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
    if metrics.copyability_score_override is not None:
        return 100.0 * clip01(metrics.copyability_score_override / 100.0)

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
    trades_30_score = clip01(metrics.trades_30d / float(MIN_TRADES_30D))
    trades_90_score = clip01(metrics.trades_90d / 90.0)
    recency_score = 1.0 - clip01(max(metrics.days_since_last_trade - MAX_DAYS_SINCE_LAST_TRADE, 0) / 25.0)
    return 100.0 * (
        0.50 * trades_30_score
        + 0.30 * trades_90_score
        + 0.20 * recency_score
    )


def return_quality_score(metrics: WalletMetrics) -> float:
    roi_180_score = clip01(metrics.roi_180 / 0.25)
    total_pnl_score = clip01(metrics.total_pnl_ratio / 0.25)
    pf_score = clip01(metrics.profit_factor / 2.5)
    lw_score = 1.0 - clip01(metrics.largest_win_share / 0.40)
    return 100.0 * (
        0.25 * roi_180_score
        + 0.25 * total_pnl_score
        + 0.25 * pf_score
        + 0.25 * lw_score
    )


def track_record_multiplier(metrics: WalletMetrics) -> float:
    return 0.5 + 0.5 * clip01(metrics.age_days / 365.0)


def data_depth_multiplier(metrics: WalletMetrics) -> float:
    return (
        0.5
        + 0.25 * clip01(metrics.closed_positions / 100.0)
        + 0.25 * clip01(metrics.unique_markets / 30.0)
    )


def score_wallet(metrics: WalletMetrics) -> WalletScoreBreakdown:
    c = consistency_score(metrics)
    d = drawdown_control_score(metrics)
    s = specialization_score(metrics)
    k = copyability_score(metrics)
    a = activity_score(metrics)
    r = return_quality_score(metrics)
    eligible, reasons = check_wallet_filters(metrics, copyability=k)

    raw_wss = (
        0.35 * c
        + 0.25 * d
        + 0.20 * s
        + 0.10 * k
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
