import unittest
from datetime import datetime, timedelta, timezone

from signals.wallet_metrics_builder import (
    build_wallet_metrics,
    _current_position_pnl_ratio,
    _primary_domain_stats,
    _profit_factor,
    _single_market_concentration,
    _total_pnl_ratio,
)
from signals.wallet_scoring import WalletMetrics, score_wallet


class TestWalletScoring(unittest.TestCase):
    def test_profit_factor_caps_wallets_with_no_losses(self) -> None:
        self.assertEqual(
            _profit_factor(
                [
                    {"realizedPnl": 10.0},
                    {"realizedPnl": 20.0},
                ]
            ),
            3.0,
        )
        self.assertEqual(_profit_factor([{"realizedPnl": 0.0}]), 0.0)

    def test_concentration_uses_notional_not_record_count(self) -> None:
        current_positions = [
            {
                "slug": "nba-finals",
                "initialValue": 900.0,
                "currentValue": 910.0,
                "cashPnl": 10.0,
            }
        ]
        small_macro_trades = [
            {
                "slug": f"cpi-print-{idx}",
                "size": 1.0,
                "price": 1.0,
            }
            for idx in range(9)
        ]

        primary_domain, primary_share = _primary_domain_stats(
            current_positions=current_positions,
            closed_positions=[],
            trades=small_macro_trades,
        )
        market_concentration = _single_market_concentration(
            current_positions=current_positions,
            closed_positions=[],
            trades=small_macro_trades,
        )

        self.assertEqual(primary_domain, "sports")
        self.assertGreater(primary_share, 0.98)
        self.assertGreater(market_concentration, 0.98)

    def test_total_pnl_ratio_includes_current_open_positions(self) -> None:
        now = datetime.now(timezone.utc)
        ratio = _total_pnl_ratio(
            closed_positions=[
                {
                    "timestamp": int((now - timedelta(days=2)).timestamp()),
                    "realizedPnl": 20.0,
                    "totalBought": 100.0,
                }
            ],
            current_positions=[
                {
                    "initialValue": 100.0,
                    "cashPnl": -30.0,
                    "redeemable": False,
                }
            ],
            now=now,
        )

        self.assertEqual(ratio, -0.05)

    def test_open_losses_reduce_wss_before_hard_filter_threshold(self) -> None:
        base = dict(
            age_days=500,
            closed_positions=160,
            unique_markets=45,
            primary_domain_share=0.60,
            single_market_concentration=0.20,
            roi_7=0.02,
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
            trades_30d=40,
            trades_90d=80,
            days_since_last_trade=2,
        )
        clean = WalletMetrics(
            **base,
            current_position_pnl_ratio=0.02,
            total_pnl_ratio=0.10,
            open_loss_exposure=0.0,
        )
        hidden_loss = WalletMetrics(
            **base,
            current_position_pnl_ratio=-0.09,
            total_pnl_ratio=-0.02,
            open_loss_exposure=1.0,
        )

        clean_result = score_wallet(clean)
        hidden_loss_result = score_wallet(hidden_loss)

        self.assertTrue(clean_result.eligible)
        self.assertTrue(hidden_loss_result.eligible)
        self.assertLess(hidden_loss_result.final_wss, clean_result.final_wss)

    def test_profile_week_and_month_pnl_are_hard_gates_when_available(self) -> None:
        metrics = WalletMetrics(
            age_days=500,
            closed_positions=160,
            unique_markets=45,
            primary_domain_share=0.60,
            single_market_concentration=0.20,
            roi_7=0.02,
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
            trades_30d=40,
            trades_90d=80,
            days_since_last_trade=2,
            leaderboard_week_pnl=1000.0,
            leaderboard_month_pnl=5000.0,
            profile_week_pnl=-1.0,
            profile_month_pnl=100.0,
        )

        result = score_wallet(metrics)

        self.assertFalse(result.eligible)
        self.assertIn("profile_week_pnl <= 0", result.filter_reasons)

    def test_profile_month_pnl_is_hard_gate_when_available(self) -> None:
        metrics = WalletMetrics(
            age_days=500,
            closed_positions=160,
            unique_markets=45,
            primary_domain_share=0.60,
            single_market_concentration=0.20,
            roi_7=0.02,
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
            trades_30d=40,
            trades_90d=80,
            days_since_last_trade=2,
            leaderboard_week_pnl=1000.0,
            leaderboard_month_pnl=5000.0,
            profile_week_pnl=100.0,
            profile_month_pnl=-1.0,
        )

        result = score_wallet(metrics)

        self.assertFalse(result.eligible)
        self.assertIn("profile_month_pnl <= 0", result.filter_reasons)

    def test_stable_wallet_scores_higher(self) -> None:
        stable = WalletMetrics(
            age_days=500,
            closed_positions=160,
            unique_markets=45,
            primary_domain_share=0.60,
            single_market_concentration=0.20,
            roi_7=0.02,
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
            trades_30d=40,
            trades_90d=80,
            days_since_last_trade=2,
        )

        unstable = WalletMetrics(
            age_days=150,
            closed_positions=45,
            unique_markets=18,
            primary_domain_share=0.36,
            single_market_concentration=0.34,
            roi_7=0.02,
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
            roi_7=0.02,
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
            trades_30d=30,
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
            roi_7=0.02,
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
        self.assertIn("trades_30d < 30", result.filter_reasons)
        self.assertIn("days_since_last_trade > 5", result.filter_reasons)

    def test_filter_rejects_recently_stale_wallet(self) -> None:
        stale = WalletMetrics(
            age_days=500,
            closed_positions=160,
            unique_markets=45,
            primary_domain_share=0.60,
            single_market_concentration=0.20,
            roi_7=0.02,
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
            current_position_pnl_ratio=0.0,
            trades_30d=40,
            trades_90d=120,
            days_since_last_trade=8,
        )

        result = score_wallet(stale)

        self.assertFalse(result.eligible)
        self.assertIn("days_since_last_trade > 5", result.filter_reasons)

    def test_filter_rejects_large_current_position_drawdown(self) -> None:
        underwater = WalletMetrics(
            age_days=500,
            closed_positions=160,
            unique_markets=45,
            primary_domain_share=0.60,
            single_market_concentration=0.20,
            roi_7=0.02,
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
            current_position_pnl_ratio=-0.40,
            trades_30d=40,
            trades_90d=80,
            days_since_last_trade=2,
        )

        result = score_wallet(underwater)

        self.assertFalse(result.eligible)
        self.assertIn("current_position_pnl_ratio < -0.10", result.filter_reasons)

    def test_filter_rejects_low_copyability_wallet(self) -> None:
        poor_copyability = WalletMetrics(
            age_days=500,
            closed_positions=160,
            unique_markets=45,
            primary_domain_share=0.60,
            single_market_concentration=0.20,
            roi_7=0.02,
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
            median_spread=0.03,
            median_liquidity=500,
            slippage_proxy=0.02,
            delay_sec=300,
            profit_factor=1.8,
            largest_win_share=0.20,
            current_position_pnl_ratio=0.0,
            trades_30d=40,
            trades_90d=80,
            days_since_last_trade=2,
        )

        result = score_wallet(poor_copyability)

        self.assertLess(result.copyability_score, 50.0)
        self.assertFalse(result.eligible)
        self.assertIn("copyability_score < 60", result.filter_reasons)

    def test_low_primary_domain_share_is_penalty_not_hard_filter(self) -> None:
        broad_generalist = WalletMetrics(
            age_days=500,
            closed_positions=160,
            unique_markets=45,
            primary_domain_share=0.20,
            single_market_concentration=0.20,
            roi_7=0.02,
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
            current_position_pnl_ratio=0.0,
            trades_30d=40,
            trades_90d=80,
            days_since_last_trade=2,
        )

        result = score_wallet(broad_generalist)

        self.assertTrue(result.eligible)
        self.assertNotIn("primary_domain_share < 0.35", result.filter_reasons)

    def test_current_position_pnl_ratio_ignores_redeemable_positions(self) -> None:
        ratio = _current_position_pnl_ratio(
            [
                {
                    "initialValue": 1000.0,
                    "cashPnl": -1000.0,
                    "redeemable": True,
                },
                {
                    "initialValue": 100.0,
                    "cashPnl": 10.0,
                    "redeemable": False,
                },
            ]
        )

        self.assertEqual(ratio, 0.10)

    def test_activity_does_not_change_wss_above_minimum_gate(self) -> None:
        base = dict(
            age_days=500,
            closed_positions=160,
            unique_markets=45,
            primary_domain_share=0.60,
            single_market_concentration=0.20,
            roi_7=0.02,
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
            days_since_last_trade=2,
        )
        barely_active = WalletMetrics(**base, trades_30d=30, trades_90d=30)
        hyper_active = WalletMetrics(**base, trades_30d=300, trades_90d=800)

        barely_result = score_wallet(barely_active)
        hyper_result = score_wallet(hyper_active)

        self.assertTrue(barely_result.eligible)
        self.assertTrue(hyper_result.eligible)
        self.assertNotEqual(barely_result.activity_score, hyper_result.activity_score)
        self.assertEqual(barely_result.final_wss, hyper_result.final_wss)

    def test_sell_only_copy_flow_is_filter_not_wss_component(self) -> None:
        base = dict(
            age_days=500,
            closed_positions=160,
            unique_markets=45,
            primary_domain_share=0.60,
            single_market_concentration=0.20,
            roi_7=0.02,
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
            trades_30d=30,
            trades_90d=80,
            days_since_last_trade=1,
        )
        sell_only = WalletMetrics(
            **base,
            buy_trades_30d=0,
            sell_trades_30d=30,
            buy_trade_share_30d=0.0,
        )
        mixed_flow = WalletMetrics(
            **base,
            buy_trades_30d=30,
            sell_trades_30d=25,
            buy_trade_share_30d=5 / 30,
        )

        sell_only_result = score_wallet(sell_only)
        mixed_result = score_wallet(mixed_flow)

        self.assertFalse(sell_only_result.eligible)
        self.assertIn("copy_flow_buy_trades_30d < 3", sell_only_result.filter_reasons)
        self.assertTrue(mixed_result.eligible)
        self.assertEqual(sell_only_result.final_wss, mixed_result.final_wss)

    def test_near_sell_only_copy_flow_is_filtered(self) -> None:
        mostly_sell = WalletMetrics(
            age_days=500,
            closed_positions=160,
            unique_markets=45,
            primary_domain_share=0.60,
            single_market_concentration=0.20,
            roi_7=0.02,
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
            trades_30d=40,
            trades_90d=40,
            buy_trades_30d=3,
            sell_trades_30d=37,
            buy_trade_share_30d=3 / 40,
            days_since_last_trade=1,
        )

        result = score_wallet(mostly_sell)

        self.assertFalse(result.eligible)
        self.assertIn("copy_flow_buy_share_30d < 0.1", result.filter_reasons)

    def test_metrics_builder_counts_recent_buy_sell_flow(self) -> None:
        now_ts = int(datetime.now(timezone.utc).timestamp())

        metrics = build_wallet_metrics(
            profile={"createdAt": "2024-01-01T00:00:00Z"},
            traded_count=3,
            current_positions=[],
            closed_positions=[],
            trades=[
                {"timestamp": now_ts - 60, "side": "BUY", "asset": "token-a"},
                {"timestamp": now_ts - 120, "side": "SELL", "asset": "token-b"},
                {"timestamp": now_ts - 31 * 24 * 60 * 60, "side": "BUY", "asset": "token-c"},
            ],
        )

        self.assertEqual(metrics.trades_30d, 2)
        self.assertEqual(metrics.trades_90d, 3)
        self.assertEqual(metrics.buy_trades_30d, 1)
        self.assertEqual(metrics.sell_trades_30d, 1)
        self.assertEqual(metrics.buy_trade_share_30d, 0.5)


if __name__ == "__main__":
    unittest.main()
