from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import execution.state_store as state_store
from execution.settlement import (
    _build_proxy_transaction_data,
    create_proxy_signature,
    run_settlement_cycle,
)
from execution.state_store import (
    get_processed_settlement,
    init_db,
    list_open_positions,
    list_trade_history,
    upsert_buy_position,
    upsert_leader_registry_row,
)


class SettlementTests(unittest.TestCase):
    def setUp(self) -> None:
        self._original_db_path = state_store.DB_PATH

    def tearDown(self) -> None:
        state_store.DB_PATH = self._original_db_path

    def test_proxy_signature_matches_official_builder_vector(self) -> None:
        from_address = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
        usdc = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
        proxy_factory = "0xaB45c5A4B0c941a2F231C04C3f49182e1A254052"
        relay = "0xae700edfd9ab986395f3999fe11177b9903a52f1"
        relay_hub = "0xD216153c06E857cD7f72665E0aF1d7D82172F494"
        private_key = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
        approve_calldata = (
            "0x095ea7b30000000000000000000000004d97dcd97ec945f40cf65f87097ace5ea0476045"
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        )

        proxy_data = _build_proxy_transaction_data(
            to=usdc,
            calldata=approve_calldata,
        )
        signature = create_proxy_signature(
            private_key=private_key,
            from_address=from_address,
            to=proxy_factory,
            data=proxy_data,
            relayer_fee="0",
            gas_price="0",
            gas_limit="85338",
            nonce="0",
            relay_hub_address=relay_hub,
            relay_address=relay,
        )

        self.assertEqual(
            signature,
            (
                "0x4c18e2d2294a00d686714aff8e7936ab657cb4655dfccb2b556efadcb7e835f8"
                "00dc2fecec69c501e29bb36ecb54b4da6b7c410c4dc740a33af2afde2b77297e1b"
            ),
        )

    def test_paper_settlement_cycle_closes_resolved_positions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_store.DB_PATH = Path(tmp) / "executor_state.db"
            init_db()
            upsert_buy_position(
                leader_wallet="wallet1",
                token_id="tokenA",
                amount_usd=5.0,
                entry_price=0.5,
                signal_id="sig-entry",
            )
            upsert_leader_registry_row(
                wallet="wallet1",
                category="ECONOMICS",
                user_name="debased",
                leader_status="ACTIVE",
                target_weight=0.25,
                target_budget_usd=25.0,
                grace_until=None,
                source_tag="test",
            )
            condition_id = "0x" + ("11" * 32)

            def fake_mark_position(position, **_kwargs):  # type: ignore[no-untyped-def]
                return {
                    **position,
                    "qty": 10.0,
                    "snapshot_status": "SETTLED",
                    "mark_source": "SETTLEMENT",
                    "settlement_price": 1.0,
                    "mark_value_mid_usd": 10.0,
                    "mark_value_bid_usd": 10.0,
                }

            config = {
                "global": {
                    "preview_mode": False,
                    "simulation": True,
                    "execution_mode": "paper",
                },
                "settlement": {
                    "enabled": True,
                },
            }

            with (
                patch("execution.settlement.mark_position", side_effect=fake_mark_position),
                patch(
                    "execution.settlement.send_trade_notification",
                    return_value=[],
                ),
            ):
                report = run_settlement_cycle(
                    config=config,
                    snapshot_loader=lambda _token_id, _side: {},
                    market_lookup=lambda _token_id: {
                        "condition_id": condition_id,
                        "question": "Will it rain?",
                        "slug": "will-it-rain",
                        "tokens": [{"token_id": "tokenA"}, {"token_id": "tokenB"}],
                    },
                    sleep_fn=lambda _seconds: None,
                )

            self.assertEqual(report["mode"], "PAPER")
            self.assertEqual(report["closed_rows"], 1)
            self.assertEqual(list_open_positions(limit=10), [])

            settlement_row = get_processed_settlement(condition_id)
            self.assertIsNotNone(settlement_row)
            self.assertEqual(settlement_row["status"], "PAPER_SETTLED")

            history = list_trade_history(limit=10)
            self.assertEqual(len(history), 1)
            self.assertEqual(history[0]["event_type"], "EXIT")
            self.assertEqual(history[0]["side"], "SELL")
            self.assertAlmostEqual(float(history[0]["realized_pnl_usd"]), 5.0, places=6)

    def test_live_external_settlement_closes_missing_exchange_position_when_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_store.DB_PATH = Path(tmp) / "executor_state.db"
            init_db()
            upsert_buy_position(
                leader_wallet="wallet1",
                token_id="tokenA",
                amount_usd=5.0,
                entry_price=0.5,
                signal_id="sig-entry",
            )
            upsert_leader_registry_row(
                wallet="wallet1",
                category="SPORTS",
                user_name="RN1",
                leader_status="ACTIVE",
                target_weight=0.25,
                target_budget_usd=25.0,
                grace_until=None,
                source_tag="test",
            )
            condition_id = "0x" + ("22" * 32)

            def fake_mark_position(position, **_kwargs):  # type: ignore[no-untyped-def]
                return {
                    **position,
                    "qty": 10.0,
                    "snapshot_status": "SETTLED",
                    "mark_source": "SETTLEMENT",
                    "settlement_price": 1.0,
                    "mark_value_mid_usd": 10.0,
                    "mark_value_bid_usd": 10.0,
                }

            config = {
                "global": {
                    "preview_mode": False,
                    "simulation": False,
                    "execution_mode": "live",
                    "live_trading_enabled": True,
                    "live_trading_ack": "I_UNDERSTAND_THIS_PLACES_REAL_ORDERS",
                },
                "settlement": {
                    "enabled": True,
                    "live_require_exchange_position": True,
                    "live_finalize_missing_exchange_position": True,
                },
            }

            with (
                patch("execution.settlement.load_executor_env") as env_mock,
                patch("execution.settlement.mark_position", side_effect=fake_mark_position),
                patch("execution.settlement.send_trade_notification", return_value=[]),
            ):
                env_mock.return_value.funder_address = "0xfunder"
                report = run_settlement_cycle(
                    config=config,
                    snapshot_loader=lambda _token_id, _side: {},
                    market_lookup=lambda _token_id: {
                        "condition_id": condition_id,
                        "question": "Will RN1 win?",
                        "slug": "will-rn1-win",
                        "tokens": [{"token_id": "tokenA"}, {"token_id": "tokenB"}],
                    },
                    exchange_positions_loader=lambda _address: [],
                    sleep_fn=lambda _seconds: None,
                )

            self.assertEqual(report["mode"], "LIVE")
            self.assertEqual(report["closed_rows"], 1)
            self.assertEqual(list_open_positions(limit=10), [])

            settlement_row = get_processed_settlement(condition_id)
            self.assertIsNotNone(settlement_row)
            self.assertEqual(settlement_row["status"], "LIVE_EXTERNAL_SETTLED")

            history = list_trade_history(limit=10)
            self.assertEqual(len(history), 1)
            self.assertEqual(history[0]["event_type"], "EXIT")
            self.assertAlmostEqual(float(history[0]["realized_pnl_usd"]), 5.0, places=6)


if __name__ == "__main__":
    unittest.main()
