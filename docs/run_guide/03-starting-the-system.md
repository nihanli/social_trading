## Part 3 — Starting the System

### Normal Startup Sequence

```
1. Launch IB Gateway / TWS (if using IBKR)          ← manual app launch
2. make up                                           ← Docker: Postgres + Redis
3. honcho start                                      ← all 6 app services
4. python -m ...execution_service [--ibkr]           ← execution (separate terminal)
```

### First-Time Only

Run these once after cloning (or after `make down -v` which wipes volumes):

```bash
make migrate                         # create DB tables
python scripts/seed_watchlist.py     # seed tickers
```

### Partial Starts

```bash
# Infrastructure + UI only (no trading)
honcho start ingest nlp signal risk ui

# Skip ingest (test signal/risk/execution with existing Redis data)
honcho start nlp signal risk ui

# Paper execution only (no IBKR)
python -m social_trading.services.execution_service

# IBKR execution
python -m social_trading.services.execution_service --ibkr
```

---

---

← [02-run-workflows.md](02-run-workflows.md) &nbsp;|&nbsp; [Index](README.md) &nbsp;|&nbsp; [04-stopping-the-system.md](04-stopping-the-system.md) →
