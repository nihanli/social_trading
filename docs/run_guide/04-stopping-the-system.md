## Part 4 — Stopping the System

### Normal Stop

```bash
Ctrl+C          # in the honcho terminal — stops all 6 services cleanly
Ctrl+C          # in the execution terminal (if running separately)
make down       # stop Docker containers (keeps data volumes)
```

### Stop Infrastructure Only (keep data)

```bash
make down       # stops Postgres + Redis, data volumes preserved
```

### Wipe Everything (full reset)

```bash
make down
docker compose down -v    # WARNING: deletes all trade history and config
```

### Emergency Stop (positions open)

**Option 1 — Streamlit UI (preferred):**
- Sidebar → click **🛑 Halt New Positions** (stops new trades, keeps existing)
- Sidebar → **Emergency Actions** → **Close ALL Positions Now** (closes everything at market)

**Option 2 — Redis command:**
```bash
redis-cli publish trading:commands '{"cmd":"CLOSE_ALL","payload":{},"ts":"now"}'
```

**Option 3 — IBKR directly:**
- Log in to IBKR Trader Workstation or web portal
- Close positions manually from the portfolio page
- Use when the system is unresponsive

**IBKR emergency phone:** +1 877-442-2757 (24/7) — ask them to freeze the account

---

---

← [03-starting-the-system.md](03-starting-the-system.md) &nbsp;|&nbsp; [Index](README.md) &nbsp;|&nbsp; [05-streamlit-ui.md](05-streamlit-ui.md) →
