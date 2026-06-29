## Part 3 — Starting the System

### Normal Startup Sequence

```
1. Launch IB Gateway / TWS (if using IBKR)          ← manual app launch
2. make test-infra                                   ← Docker: Postgres + Redis (test)
3. honcho start -e .env.test                         ← all 6 app services
4. python -m ...execution_service [--ibkr]           ← execution (separate terminal)
```

### First-Time Only

Run these once after cloning (or after wiping volumes):

```bash
make test-infra
make migrate-test                     # create DB tables in trading_test
python scripts/seed_watchlist.py      # seed tickers
```

### Partial Starts

```bash
# Infrastructure + UI only (no trading)
honcho start -e .env.test ingest nlp signal risk ui

# Skip ingest (test signal/risk/execution with existing Redis data)
honcho start -e .env.test nlp signal risk ui

# Paper execution only (no IBKR)
python -m social_trading.services.execution_service

# IBKR execution
python -m social_trading.services.execution_service --ibkr
```

### Production Start

```
1. Launch live IB Gateway / TWS (port 7496)
2. make prod-infra                                   ← Docker: Postgres + Redis (prod)
3. honcho start -e .env.prod                         ← all 6 app services
4. python -m ...execution_service --ibkr             ← execution with live IBKR
```

---

---

← [02-run-workflows.md](02-run-workflows.md) &nbsp;|&nbsp; [Index](README.md) &nbsp;|&nbsp; [04-stopping-the-system.md](04-stopping-the-system.md) →
