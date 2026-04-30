# Polymarket Copy-Trading Bot

Python research and preview execution framework for Polymarket copy-trading.

The project is intentionally conservative by default:

- `config/executor.toml` runs in `PREVIEW` mode against `data/executor_state.db`.
- `config/executor.paper.toml` runs in `PAPER` mode against `data/executor_state_paper.db`.
- `config/executor.live.toml` targets live trading against `data/executor_state_live.db` and derives capital/risk limits from the account collateral balance, but still ships with `live_trading_enabled = false`.
- `LIVE` requires an explicit config switch, verified funding checks, live acknowledgement, and clean readiness checks.

## Runtime Checks

Show the currently resolved mode and SQLite state DB:

```bash
env PYTHONDONTWRITEBYTECODE=1 .venv/bin/python app/runtime_state_check.py
```

Show the paper runtime state:

```bash
env PYTHONDONTWRITEBYTECODE=1 \
  POLY_EXECUTOR_CONFIG_PATH=config/executor.paper.toml \
  .venv/bin/python app/runtime_state_check.py
```

## Paper Soak

Prepare the isolated paper DB leader registry from the live allocation:

```bash
env PYTHONDONTWRITEBYTECODE=1 \
  POLY_EXECUTOR_CONFIG_PATH=config/executor.paper.toml \
  .venv/bin/python app/apply_rebalance_lifecycle.py
```

Run one paper-soak cycle:

```bash
env PYTHONDONTWRITEBYTECODE=1 \
  POLY_EXECUTOR_CONFIG_PATH=config/executor.paper.toml \
  .venv/bin/python app/paper_soak_cycle.py
```

Run a repeated paper-soak loop:

```bash
env PYTHONDONTWRITEBYTECODE=1 \
  POLY_EXECUTOR_CONFIG_PATH=config/executor.paper.toml \
  .venv/bin/python app/paper_soak_loop.py --interval-sec 30
```

Monitor paper-soak progress without making network calls:

```bash
env PYTHONDONTWRITEBYTECODE=1 \
  POLY_EXECUTOR_CONFIG_PATH=config/executor.paper.toml \
  .venv/bin/python app/paper_soak_status.py
```

Save the latest status JSON and append a compact CSV history row:

```bash
env PYTHONDONTWRITEBYTECODE=1 \
  POLY_EXECUTOR_CONFIG_PATH=config/executor.paper.toml \
  .venv/bin/python app/paper_soak_status.py --save
```

Check whether the paper-soak dataset is good enough for cutover:

```bash
env PYTHONDONTWRITEBYTECODE=1 \
  POLY_EXECUTOR_CONFIG_PATH=config/executor.paper.toml \
  .venv/bin/python app/cutover_readiness_check.py
```

## Live Readiness

Live should remain off until both checks are clean:

```bash
env PYTHONDONTWRITEBYTECODE=1 .venv/bin/python app/executor_health_check.py
env PYTHONDONTWRITEBYTECODE=1 .venv/bin/python app/live_readiness_check.py
```

Run the live-profile checks against the isolated live DB:

```bash
env PYTHONDONTWRITEBYTECODE=1 \
  POLY_EXECUTOR_CONFIG_PATH=config/executor.live.toml \
  .venv/bin/python app/runtime_state_check.py

env PYTHONDONTWRITEBYTECODE=1 \
  POLY_EXECUTOR_CONFIG_PATH=config/executor.live.toml \
  .venv/bin/python app/live_readiness_check.py
```

Expected pre-live blockers include missing/failed CLOB credential verification, unresolved unverified orders, exchange reconciliation issues, and preview/paper state leaking into the live DB.

Build the current alert snapshot:

```bash
env PYTHONDONTWRITEBYTECODE=1 \
  POLY_EXECUTOR_CONFIG_PATH=config/executor.live.toml \
  .venv/bin/python app/executor_alerts.py
```

Send the alert snapshot to configured Telegram/Discord/email/generic webhooks:

```bash
env PYTHONDONTWRITEBYTECODE=1 \
  POLY_EXECUTOR_CONFIG_PATH=config/executor.live.toml \
  .venv/bin/python app/executor_alerts.py --deliver
```

Configure destinations through env vars such as `POLY_ALERT_TELEGRAM_BOT_TOKEN`, `POLY_ALERT_TELEGRAM_CHAT_ID`, `POLY_ALERT_DISCORD_WEBHOOK_URL`, `POLY_ALERT_EMAIL_WEBHOOK_URL`, or `POLY_ALERT_GENERIC_WEBHOOK_URL`. Delivery is off until `[alert_delivery].enabled = true`. Empty "no alerts" snapshots are not sent unless `[alert_delivery].send_empty_alerts = true`; filled entries/exits can be sent with `[alert_delivery].notify_trades = true`.

Run the Telegram command bot for status, positions, leaders, and 24h activity:

```bash
env PYTHONDONTWRITEBYTECODE=1 \
  POLY_EXECUTOR_CONFIG_PATH=config/executor.live.toml \
  .venv/bin/python app/telegram_bot.py
```

View or clear the runtime kill-switch lock:

```bash
env PYTHONDONTWRITEBYTECODE=1 \
  POLY_EXECUTOR_CONFIG_PATH=config/executor.live.toml \
  .venv/bin/python app/runtime_lock_control.py

env PYTHONDONTWRITEBYTECODE=1 \
  POLY_EXECUTOR_CONFIG_PATH=config/executor.live.toml \
  .venv/bin/python app/runtime_lock_control.py --clear
```

Run a live smoke test without submitting an order:

```bash
env PYTHONDONTWRITEBYTECODE=1 \
  POLY_EXECUTOR_CONFIG_PATH=config/executor.live.toml \
  .venv/bin/python app/live_smoke_test.py
```

The submit path is intentionally guarded by `--submit`, a token id, an amount, enabled live config, and the live trading acknowledgement.

Back up the configured live state DB:

```bash
env PYTHONDONTWRITEBYTECODE=1 \
  POLY_EXECUTOR_CONFIG_PATH=config/executor.live.toml \
  .venv/bin/python app/backup_live_state.py --label before_live_start
```

Recover unknown live submissions only after the exchange reports a verified fill amount and fill price:

```bash
env PYTHONDONTWRITEBYTECODE=1 \
  POLY_EXECUTOR_CONFIG_PATH=config/executor.live.toml \
  .venv/bin/python app/recover_unverified_orders.py

env PYTHONDONTWRITEBYTECODE=1 \
  POLY_EXECUTOR_CONFIG_PATH=config/executor.live.toml \
  .venv/bin/python app/recover_unverified_orders.py \
  --apply --ack APPLY_VERIFIED_LIVE_RECOVERY
```

## Safety Invariants

- Do not process the same signal twice.
- Re-entry after full close must reopen the existing `(leader_wallet, token_id)` state row.
- SELL can partially reduce positions.
- Sizing prefers leader trade notional and falls back to budget only when notional is missing.
- Observation replay only uses replayable statuses.
- Paper/live modes must use isolated SQLite state DBs.
- LIVE BUYs are blocked while critical executor alerts exist; SELL/exit handling remains available.
- Critical alerts activate a runtime lock that survives process restarts until cleared manually.
- Live submit now polls exchange order status before leaving an order in `LIVE_SUBMITTED_UNVERIFIED`.
- Live DB backups are available before guarded submits and after live polling cycles.
- LIVE state is updated only after verified live fills; unknown submissions must be recovered or manually reviewed.
