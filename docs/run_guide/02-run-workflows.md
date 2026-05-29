## Part 2 — Run Workflows

Three distinct operating modes depending on your goal.

---

### Workflow A — Debug Run

**Purpose:** Test code changes, explore the pipeline, no real APIs or money needed.

**What runs:** All services on your machine. Execution in paper mode (simulated, no IBKR).
Social data sources disabled (no API keys needed).

**Terminal 1 — Infrastructure:**
```bash
make up
```

**Terminal 2 — All services:**
```bash
source .venv/bin/activate
honcho start ingest nlp signal risk ui
```

> With no API keys set, ingest produces no posts. To test the pipeline
> end-to-end, publish a synthetic post directly to Redis:
> ```bash
> redis-cli xadd raw_social '*' ticker AAPL text "AAPL is going to the moon!" source twitter score 0 ts 0
> ```

**Execution service (paper, no IBKR):**
```bash
# In a second terminal:
source .venv/bin/activate
python -m social_trading.services.execution_service
```

**Stop:** `Ctrl+C` in each terminal, then `make down` to stop Docker.

---

### Workflow B — QA / Paper Trading Run

**Purpose:** 5-day paper dry run — real social data, real market data, simulated trades via IBKR paper account.
This is the gate before going live.

**Prerequisites:** API keys in `.env`, IB Gateway running and logged in to paper account.

**Step 1 — Start infrastructure:**
```bash
make up
docker compose ps    # confirm postgres and redis show (healthy)
```

**Step 2 — Verify IB Gateway is reachable:**
```bash
nc -z 127.0.0.1 7497 && echo "TWS reachable" || echo "NOT reachable"
# OR for Gateway:
nc -z 127.0.0.1 4002 && echo "Gateway reachable" || echo "NOT reachable"
```

**Step 3 — Start all services in one terminal:**
```bash
source .venv/bin/activate
honcho start ingest nlp signal risk ui
```

Expected startup log (within 60 seconds):
```
13:01:00 ingest.1    | Polling X for AAPL...
13:01:01 nlp.1       | Classified 12 posts
13:01:02 signal.1    | LONG signal AAPL quality=0.72
13:01:03 risk.1      | Gate PASSED qty=15
13:01:04 ui.1        | You can now view your Streamlit app in your browser.
```

**Step 4 — Start execution with IBKR (second terminal):**
```bash
source .venv/bin/activate
python -m social_trading.services.execution_service --ibkr
```

Expected: `Connected to IBKR port=7497 clientId=10`

**Step 5 — Start observability (optional):**
```bash
docker compose up -d prometheus grafana
```

**Open the UI:** http://localhost:8501

---

### Workflow C — Production Live Run

**Prerequisites:** Completed 5-day paper run with Sharpe ≥ 0.5. See Part 7 (paper→live checklist).

**Changes from paper run:**

1. Tighten risk parameters in Streamlit → Config page (see Part 7.2)

2. Update `.env`:
   ```dotenv
   IBKR_PORT=4001        # IB Gateway live port
   TRADING_MODE=live
   ```

3. Switch IB Gateway to live account login (port 4001)

4. Start exactly as Workflow B — the `--ibkr` flag reads `IBKR_PORT` from `.env`

5. Seed conservative watchlist:
   ```bash
   python scripts/seed_watchlist.py --tickers AAPL MSFT SPY QQQ
   ```

---

---

← [01-environment-setup.md](01-environment-setup.md) &nbsp;|&nbsp; [Index](README.md) &nbsp;|&nbsp; [03-starting-the-system.md](03-starting-the-system.md) →
