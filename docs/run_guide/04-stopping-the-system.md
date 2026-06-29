## Part 4 — Stopping the System

### Normal Stop

**Test environment:**
```bash
Ctrl+C              # in the honcho terminal — stops all services cleanly
Ctrl+C              # in the execution terminal (if running separately)
make test-infra-down   # stop test Docker containers (keeps data volumes)
```

**Production environment:**
```bash
Ctrl+C              # in the honcho terminal
Ctrl+C              # in the execution terminal
make prod-infra-down   # stop prod Docker containers (keeps data volumes)
```

### Stop Infrastructure Only (keep data)

```bash
make test-infra-down    # stops test Postgres + Redis, data volumes preserved
make prod-infra-down    # stops prod Postgres + Redis, data volumes preserved
```

### Wipe Everything (full reset)

```bash
# Wipe test environment
make test-infra-down
docker compose -p social_trading_test -f docker-compose.test.yml down -v  # WARNING: deletes test data

# Wipe prod environment
make prod-infra-down
docker compose -p social_trading_prod -f docker-compose.prod.yml down -v  # WARNING: deletes prod data
```

### Emergency Stop (positions open)

**Option 1 — Streamlit UI (preferred):**
- Sidebar → click **🛑 Halt New Positions** (stops new trades, keeps existing)
- Sidebar → **Emergency Actions** → **Close ALL Positions Now** (closes everything at market)

**Option 2 — Redis command:**
```bash
# Test environment (Redis :6379):
redis-cli -p 6379 publish trading:commands '{"cmd":"CLOSE_ALL","payload":{},"ts":"now"}'

# Prod environment (Redis :6380):
redis-cli -p 6380 publish trading:commands '{"cmd":"CLOSE_ALL","payload":{},"ts":"now"}'
```

**Option 3 — IBKR directly:**
- Log in to IBKR Trader Workstation or web portal
- Close positions manually from the portfolio page
- Use when the system is unresponsive

**IBKR emergency phone:** +1 877-442-2757 (24/7) — ask them to freeze the account

---

---

← [03-starting-the-system.md](03-starting-the-system.md) &nbsp;|&nbsp; [Index](README.md) &nbsp;|&nbsp; [05-streamlit-ui.md](05-streamlit-ui.md) →
