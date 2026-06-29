# Part 11 — Test / Production Environments

The system supports two fully isolated environments running simultaneously on the same machine:

| | Test | Production |
|---|---|---|
| Docker project | `social_trading_test` | `social_trading_prod` |
| Postgres container | `...-postgres-1` → `:5432` | `...-postgres-1` → `:5433` |
| Redis container | `...-redis-1` → `:6379` | `...-redis-1` → `:6380` |
| Database | `trading_test` | `trading_prod` |
| Services | `honcho start -e .env.test` | `honcho start -e .env.prod` |
| IBKR | port `7497` (paper TWS) | port `7496` (live TWS) |
| Streamlit UI | http://localhost:8501 | http://localhost:8502 |
| Config file | `.env.test` | `.env.prod` |

---

## Running Test Only

```bash
# 1. Start infrastructure
make test-infra

# 2. Start services (one terminal)
source .venv/bin/activate
honcho start -e .env.test

# 3. Start execution (second terminal)
source .venv/bin/activate
python -m social_trading.services.execution_service --ibkr

# Stop
Ctrl+C (in each terminal)
make test-infra-down
```

---

## Running Production Only

```bash
# 1. Start infrastructure
make prod-infra

# 2. Start services
source .venv/bin/activate
honcho start -e .env.prod

# 3. Start execution
source .venv/bin/activate
python -m social_trading.services.execution_service --ibkr

# Stop
Ctrl+C (in each terminal)
make prod-infra-down
```

---

## Running Both Simultaneously

```bash
# 1. Start both Docker stacks
make test-infra && make prod-infra

# Terminal 2 — test services  (UI at :8501)
honcho start -e .env.test

# Terminal 3 — prod services  (UI at :8502)
honcho start -e .env.prod

# Terminal 4 — test execution (→ TWS :7497)
python -m social_trading.services.execution_service --ibkr

# Terminal 5 — prod execution (→ TWS :7496)
python -m social_trading.services.execution_service --ibkr
```

> Each execution service reads `IBKR_PORT` from the `.env.*` file honcho loaded,
> so they connect to different TWS instances automatically.

---

## Switching Between Environments

There is nothing to "switch" — the environment is determined by which `.env.*` file
you pass to honcho. Both can run at the same time.

If you want to stop test and run prod only:
```bash
Ctrl+C     # in the test honcho terminal
make test-infra-down
# (prod keeps running unchanged)
```

---

## Starting Individual Services

```bash
# Test: start only ingest + nlp (no signal/risk/execution/ui)
honcho start -e .env.test ingest nlp

# Prod: start only the UI
honcho start -e .env.prod ui

# Test: skip ingest (use existing Redis data)
honcho start -e .env.test nlp signal risk ui
```

---

## Syncing Reference Data Test → Prod

When you want prod to benefit from weeks of test-env signal learning:

```bash
./scripts/sync_to_prod.sh
```

This copies `social_raw`, `sentiment_scores`, `sentiment_aggregates`, and `signals`
from `trading_test` to `trading_prod`. Trade history (`trades`, `positions`,
`account_equity`) is never synced — it remains environment-specific.

---

## Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Docker (two isolated project namespaces)                               │
│                                                                         │
│  Project: social_trading_test          Project: social_trading_prod     │
│  ┌──────────────────────────┐          ┌────────────────────────────┐   │
│  │ postgres  host:5432      │          │ postgres  host:5433         │   │
│  │ redis     host:6379      │          │ redis     host:6380         │   │
│  │ volume: *_test_pg_data   │          │ volume: *_prod_pg_data      │   │
│  │ volume: *_test_redis_data│          │ volume: *_prod_redis_data   │   │
│  └──────────────────────────┘          └────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘

  Terminal 2 (test)                       Terminal 3 (prod)
  ─────────────────────────────           ─────────────────────────────
  honcho start -e .env.test               honcho start -e .env.prod
    ingest / nlp / signal / risk            ingest / nlp / signal / risk
    execution → TWS :7497                   execution → TWS :7496
    persistence → trading_test              persistence → trading_prod
    ui → :8501                              ui → :8502
  Ctrl+C stops test services              Ctrl+C stops prod services
```

---

← [10-reference.md](10-reference.md) &nbsp;|&nbsp; [Index](README.md)
