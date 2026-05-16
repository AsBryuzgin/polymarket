from signals.wallet_scoring import WalletMetrics, score_wallet


def main() -> None:
    metrics = WalletMetrics(
        age_days=420,
        closed_positions=135,
        unique_markets=42,
        primary_domain_share=0.58,
        single_market_concentration=0.18,
        roi_30=0.06,
        roi_90=0.14,
        roi_180=0.22,
        monthly_roi_last_6=[0.03, 0.02, 0.05, 0.01, 0.04, 0.03],
        negative_monthly_roi_last_12=[-0.01, -0.02, -0.015],
        primary_domain_roi_30=0.05,
        primary_domain_roi_90=0.13,
        primary_domain_roi_180=0.20,
        max_drawdown=0.08,
        longest_loss_streak=2,
        median_spread=0.01,
        median_liquidity=18000,
        slippage_proxy=0.006,
        delay_sec=45,
        profit_factor=1.9,
        largest_win_share=0.18,
        trades_30d=14,
        trades_90d=34,
        days_since_last_trade=1,
    )

    result = score_wallet(metrics)

    print("=== Wallet Stability Score Demo ===")
    print(f"eligible: {result.eligible}")
    print(f"filter_reasons: {result.filter_reasons}")
    print(f"consistency_score: {result.consistency_score}")
    print(f"drawdown_score: {result.drawdown_score}")
    print(f"specialization_score: {result.specialization_score}")
    print(f"copyability_score: {result.copyability_score}")
    print(f"activity_score: {result.activity_score}")
    print(f"return_quality_score: {result.return_quality_score}")
    print(f"raw_wss: {result.raw_wss}")
    print(f"track_record_multiplier: {result.track_record_multiplier}")
    print(f"data_depth_multiplier: {result.data_depth_multiplier}")
    print(f"final_wss: {result.final_wss}")


if __name__ == "__main__":
    main()
