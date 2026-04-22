import unittest

from signals.wallet_scoring import WalletMetrics, score_wallet


class TestWalletScoring(unittest.TestCase):
    def test_stable_wallet_scores_higher(self) -> None:
        stable = WalletMetrics(
            age_days=500,
            closed_positions=160,
            unique_markets=45,
            primary_domain_share=0.60,
            single_market_concentration=0.20,
            roi_30=0.05,
            roi_90=0.10,
            roi_180=0.18,
            monthly_roi_last_6=[0.03, 0.02, 0.04, 0.01, 0.03, 0.02],
            negative_monthly_roi_last_12=[-0.01, -0.015],
            primary_domain_roi_30=0.04,
            primary_domain_roi_90=0.11,
            primary_domain_roi_180=0.19,
            max_drawdown=0.07,
            longest_loss_streak=2,
            median_spread=0.01,
            median_liquidity=22000,
            slippage_proxy=0.005,
            delay_sec=40,
            profit_factor=1.8,
            largest_win_share=0.20,
            trades_30d=12,
            trades_90d=30,
            days_since_last_trade=2,
        )

        unstable = WalletMetrics(
            age_days=150,
            closed_positions=45,
            unique_markets=18,
            primary_domain_share=0.36,
            single_market_concentration=0.34,
            roi_30=0.35,
            roi_90=-0.10,
            roi_180=0.50,
            monthly_roi_last_6=[0.30, -0.15, 0.25, -0.10, 0.18, -0.12],
            negative_monthly_roi_last_12=[-0.15, -0.12, -0.10],
            primary_domain_roi_30=0.20,
            primary_domain_roi_90=-0.08,
            primary_domain_roi_180=0.40,
            max_drawdown=0.28,
            longest_loss_streak=5,
            median_spread=0.04,
            median_liquidity=1500,
            slippage_proxy=0.03,
            delay_sec=220,
            profit_factor=1.2,
            largest_win_share=0.62,
            trades_30d=1,
            trades_90d=4,
            days_since_last_trade=28,
        )

        stable_result = score_wallet(stable)
        unstable_result = score_wallet(unstable)

        self.assertTrue(stable_result.final_wss > unstable_result.final_wss)

    def test_filter_rejects_young_wallet(self) -> None:
        young = WalletMetrics(
            age_days=40,
            closed_positions=12,
            unique_markets=6,
            primary_domain_share=0.30,
            single_market_concentration=0.50,
            roi_30=0.08,
            roi_90=0.00,
            roi_180=0.00,
            monthly_roi_last_6=[0.08],
            negative_monthly_roi_last_12=[],
            primary_domain_roi_30=0.08,
            primary_domain_roi_90=0.00,
            primary_domain_roi_180=0.00,
            max_drawdown=0.05,
            longest_loss_streak=1,
            median_spread=0.01,
            median_liquidity=5000,
            slippage_proxy=0.005,
            delay_sec=30,
            profit_factor=2.0,
            largest_win_share=0.10,
            trades_30d=5,
            trades_90d=10,
            days_since_last_trade=3,
        )

        result = score_wallet(young)

        self.assertFalse(result.eligible)
        self.assertTrue(len(result.filter_reasons) > 0)

    def test_filter_rejects_inactive_wallet(self) -> None:
        inactive = WalletMetrics(
            age_days=500,
            closed_positions=160,
            unique_markets=45,
            primary_domain_share=0.60,
            single_market_concentration=0.20,
            roi_30=0.05,
            roi_90=0.10,
            roi_180=0.18,
            monthly_roi_last_6=[0.03, 0.02, 0.04, 0.01, 0.03, 0.02],
            negative_monthly_roi_last_12=[-0.01, -0.015],
            primary_domain_roi_30=0.04,
            primary_domain_roi_90=0.11,
            primary_domain_roi_180=0.19,
            max_drawdown=0.07,
            longest_loss_streak=2,
            median_spread=0.01,
            median_liquidity=22000,
            slippage_proxy=0.005,
            delay_sec=40,
            profit_factor=1.8,
            largest_win_share=0.20,
            trades_30d=0,
            trades_90d=1,
            days_since_last_trade=60,
        )

        result = score_wallet(inactive)

        self.assertFalse(result.eligible)
        self.assertIn("trades_90d < 3", result.filter_reasons)
        self.assertIn("days_since_last_trade > 45", result.filter_reasons)


if __name__ == "__main__":
    unittest.main()
