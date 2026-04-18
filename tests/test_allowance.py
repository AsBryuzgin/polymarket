from __future__ import annotations

import unittest

from execution.allowance import (
    FundingSnapshot,
    PreflightDecision,
    evaluate_live_funding_preflight,
    parse_balance_allowance_response,
)
from execution.order_router import LIVE_TRADING_ACK, execute_market_order, parse_live_order_response
from execution.order_router import resolve_execution_mode


class AllowancePreflightTests(unittest.TestCase):
    def test_parse_balance_allowance_from_clob_base_units(self) -> None:
        snapshot = parse_balance_allowance_response(
            {"balance": "2500000", "allowance": "1200000"},
            decimals=6,
        )

        self.assertEqual(snapshot.balance_usd, 2.5)
        self.assertEqual(snapshot.allowance_usd, 1.2)

    def test_buy_preflight_blocks_insufficient_balance(self) -> None:
        decision = evaluate_live_funding_preflight(
            config={"funding": {"cash_reserve_usd": 1.0}},
            side="BUY",
            amount_usd=2.0,
            snapshot_loader=lambda _config: FundingSnapshot(
                balance_usd=2.5,
                allowance_usd=10.0,
            ),
        )

        self.assertFalse(decision.allowed)
        self.assertIn("insufficient collateral balance", decision.reason)

    def test_buy_preflight_blocks_insufficient_allowance(self) -> None:
        decision = evaluate_live_funding_preflight(
            config={"funding": {"cash_reserve_usd": 0.0}},
            side="BUY",
            amount_usd=2.0,
            snapshot_loader=lambda _config: FundingSnapshot(
                balance_usd=10.0,
                allowance_usd=1.5,
            ),
        )

        self.assertFalse(decision.allowed)
        self.assertIn("insufficient collateral allowance", decision.reason)

    def test_buy_preflight_allows_sufficient_funding(self) -> None:
        decision = evaluate_live_funding_preflight(
            config={"funding": {"cash_reserve_usd": 1.0}},
            side="BUY",
            amount_usd=2.0,
            snapshot_loader=lambda _config: FundingSnapshot(
                balance_usd=3.5,
                allowance_usd=2.0,
            ),
        )

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.details["required_usd"], 3.0)

    def test_buy_preflight_uses_cash_reserve_pct_from_balance(self) -> None:
        decision = evaluate_live_funding_preflight(
            config={"funding": {"cash_reserve_pct": 0.05}},
            side="BUY",
            amount_usd=5.0,
            snapshot_loader=lambda _config: FundingSnapshot(
                balance_usd=100.0,
                allowance_usd=5.0,
            ),
        )

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.details["cash_reserve_usd"], 5.0)
        self.assertEqual(decision.details["required_usd"], 10.0)

    def test_live_router_blocks_when_funding_preflight_fails(self) -> None:
        config = {
            "global": {
                "simulation": False,
                "preview_mode": False,
                "execution_mode": "live",
                "live_trading_enabled": True,
                "live_trading_ack": LIVE_TRADING_ACK,
            }
        }

        result = execute_market_order(
            config=config,
            token_id="tokenA",
            amount_usd=2.0,
            side="BUY",
            preview_fn=lambda **_kwargs: {"unexpected": True},
            live_preflight_fn=lambda **_kwargs: PreflightDecision(
                allowed=False,
                reason="insufficient collateral balance 1.00 for required 3.00",
                details={"balance_usd": 1.0, "required_usd": 3.0},
            ),
        )

        self.assertFalse(result.accepted)
        self.assertEqual(result.status, "LIVE_PREFLIGHT_BLOCKED")
        self.assertEqual(result.details["balance_usd"], 1.0)

    def test_execution_mode_parses_string_booleans_safely(self) -> None:
        mode = resolve_execution_mode(
            {
                "global": {
                    "simulation": "false",
                    "preview_mode": "false",
                    "execution_mode": "live",
                }
            }
        )

        self.assertEqual(mode, "LIVE")

    def test_live_router_accepts_verified_fill_amount_only(self) -> None:
        config = {
            "global": {
                "simulation": False,
                "preview_mode": False,
                "execution_mode": "live",
                "live_trading_enabled": True,
                "live_trading_ack": LIVE_TRADING_ACK,
            },
            "live_execution": {"require_verified_fill": True},
        }

        result = execute_market_order(
            config=config,
            token_id="tokenA",
            amount_usd=2.0,
            side="BUY",
            preview_fn=lambda **_kwargs: {"unexpected": True},
            live_preflight_fn=lambda **_kwargs: PreflightDecision(allowed=True, reason="ok"),
            live_order_fn=lambda **_kwargs: {
                "post_order_response": {
                    "success": True,
                    "orderID": "order1",
                    "filled_amount_usd": "1.75",
                    "status": "FILLED",
                }
            },
        )

        self.assertTrue(result.accepted)
        self.assertEqual(result.status, "LIVE_FILLED")
        self.assertEqual(result.order_id, "order1")
        self.assertEqual(result.fill_amount_usd, 1.75)
        self.assertEqual(result.details["fill_price"], None)

    def test_live_response_computes_fill_notional_from_size_matched_and_price(self) -> None:
        result = parse_live_order_response(
            raw_response={
                "id": "order2",
                "status": "FILLED",
                "success": True,
                "price": "0.42",
                "size_matched": "5",
            },
            requested_amount_usd=2.5,
            require_verified_fill=True,
        )

        self.assertTrue(result.accepted)
        self.assertEqual(result.status, "LIVE_FILLED")
        self.assertEqual(result.order_id, "order2")
        self.assertEqual(result.fill_amount_usd, 2.1)
        self.assertEqual(result.details["fill_price"], 0.42)

    def test_live_router_does_not_accept_unverified_fill(self) -> None:
        config = {
            "global": {
                "simulation": False,
                "preview_mode": False,
                "execution_mode": "live",
                "live_trading_enabled": True,
                "live_trading_ack": LIVE_TRADING_ACK,
            },
            "live_execution": {"require_verified_fill": True},
        }

        result = execute_market_order(
            config=config,
            token_id="tokenA",
            amount_usd=2.0,
            side="BUY",
            preview_fn=lambda **_kwargs: {"unexpected": True},
            live_preflight_fn=lambda **_kwargs: PreflightDecision(allowed=True, reason="ok"),
            live_order_fn=lambda **_kwargs: {
                "post_order_response": {
                    "success": True,
                    "orderID": "order1",
                    "status": "MATCHED",
                }
            },
        )

        self.assertFalse(result.accepted)
        self.assertEqual(result.status, "LIVE_SUBMITTED_UNVERIFIED")
        self.assertEqual(result.fill_amount_usd, 0.0)

    def test_live_router_polls_after_unverified_submit_until_fill(self) -> None:
        config = {
            "global": {
                "simulation": False,
                "preview_mode": False,
                "execution_mode": "live",
                "live_trading_enabled": True,
                "live_trading_ack": LIVE_TRADING_ACK,
            },
            "live_execution": {
                "require_verified_fill": True,
                "post_submit_poll_attempts": 2,
                "post_submit_poll_interval_sec": 0.0,
            },
        }

        responses = iter(
            [
                {"orderID": "order1", "status": "MATCHED", "success": True},
                {
                    "orderID": "order1",
                    "status": "FILLED",
                    "success": True,
                    "price": "0.25",
                    "size_matched": "8",
                },
            ]
        )

        result = execute_market_order(
            config=config,
            token_id="tokenA",
            amount_usd=2.0,
            side="BUY",
            preview_fn=lambda **_kwargs: {"unexpected": True},
            live_preflight_fn=lambda **_kwargs: PreflightDecision(allowed=True, reason="ok"),
            live_order_fn=lambda **_kwargs: {
                "post_order_response": {
                    "success": True,
                    "orderID": "order1",
                    "status": "MATCHED",
                }
            },
            live_order_status_fn=lambda _order_id: next(responses),
            sleep_fn=lambda _seconds: None,
        )

        self.assertTrue(result.accepted)
        self.assertEqual(result.status, "LIVE_FILLED")
        self.assertEqual(result.fill_amount_usd, 2.0)
        self.assertEqual(result.details["fill_price"], 0.25)
        self.assertEqual(result.details["post_submit_poll_attempts"], 2)

    def test_live_router_polls_after_unverified_submit_until_rejected(self) -> None:
        config = {
            "global": {
                "simulation": False,
                "preview_mode": False,
                "execution_mode": "live",
                "live_trading_enabled": True,
                "live_trading_ack": LIVE_TRADING_ACK,
            },
            "live_execution": {
                "require_verified_fill": True,
                "post_submit_poll_attempts": 1,
                "post_submit_poll_interval_sec": 0.0,
            },
        }

        result = execute_market_order(
            config=config,
            token_id="tokenA",
            amount_usd=2.0,
            side="BUY",
            preview_fn=lambda **_kwargs: {"unexpected": True},
            live_preflight_fn=lambda **_kwargs: PreflightDecision(allowed=True, reason="ok"),
            live_order_fn=lambda **_kwargs: {
                "post_order_response": {
                    "success": True,
                    "orderID": "order1",
                    "status": "MATCHED",
                }
            },
            live_order_status_fn=lambda _order_id: {
                "orderID": "order1",
                "status": "REJECTED",
                "success": False,
            },
            sleep_fn=lambda _seconds: None,
        )

        self.assertFalse(result.accepted)
        self.assertEqual(result.status, "LIVE_REJECTED")
        self.assertEqual(result.order_id, "order1")

    def test_live_router_reports_submit_errors(self) -> None:
        config = {
            "global": {
                "simulation": False,
                "preview_mode": False,
                "execution_mode": "live",
                "live_trading_enabled": True,
                "live_trading_ack": LIVE_TRADING_ACK,
            }
        }

        def fail_submit(**_kwargs):
            raise RuntimeError("post failed")

        result = execute_market_order(
            config=config,
            token_id="tokenA",
            amount_usd=2.0,
            side="BUY",
            preview_fn=lambda **_kwargs: {"unexpected": True},
            live_preflight_fn=lambda **_kwargs: PreflightDecision(allowed=True, reason="ok"),
            live_order_fn=fail_submit,
        )

        self.assertFalse(result.accepted)
        self.assertEqual(result.status, "LIVE_SUBMIT_ERROR")
        self.assertIn("post failed", result.reason)

    def test_live_router_rejects_failed_response_even_with_amount(self) -> None:
        config = {
            "global": {
                "simulation": False,
                "preview_mode": False,
                "execution_mode": "live",
                "live_trading_enabled": True,
                "live_trading_ack": LIVE_TRADING_ACK,
            }
        }

        result = execute_market_order(
            config=config,
            token_id="tokenA",
            amount_usd=2.0,
            side="BUY",
            preview_fn=lambda **_kwargs: {"unexpected": True},
            live_preflight_fn=lambda **_kwargs: PreflightDecision(allowed=True, reason="ok"),
            live_order_fn=lambda **_kwargs: {
                "post_order_response": {
                    "success": False,
                    "orderID": "order1",
                    "filled_amount_usd": "2.0",
                    "status": "REJECTED",
                }
            },
        )

        self.assertFalse(result.accepted)
        self.assertEqual(result.status, "LIVE_REJECTED")


if __name__ == "__main__":
    unittest.main()
