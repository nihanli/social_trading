## Part 2 — Run Workflows

Four distinct operating modes depending on your goal.

---

### Workflow A — Debug Run

**Purpose:** Test code changes, explore the pipeline, no real APIs or money needed.

**What runs:** All services on your machine. Execution in paper mode (simulated, no IBKR).
Social data sources disabled (no API keys needed).

**Terminal 1 — Infrastructure:**
```bash
make test-infra
```

**Terminal 2 — All services:**
```bash
source .venv/bin/activate
honcho start -e .env.test ingest nlp signal risk ui
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

**Stop:** `Ctrl+C` in each terminal, then `make test-infra-down` to stop Docker.

---

### Workflow B — QA / Paper Trading Run

**Purpose:** 5-day paper dry run — real social data, real market data, simulated trades via IBKR paper account.
This is the gate before going live.

**Prerequisites:** API keys in `.env.test`, IB Gateway running and logged in to paper account.

**Step 1 — Start test infrastructure:**
```bash
make test-infra
docker compose -p social_trading_test -f docker-compose.test.yml ps   # confirm healthy
```

**Step 2 — Verify IB Gateway is reachable:**
```bash
nc -z 127.0.0.1 7497 && echo "TWS reachable" || echo "NOT reachable"
```

**Step 3 — Start all services in one terminal:**
```bash
source .venv/bin/activate
honcho start -e .env.test
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

**Open the UI:** http://localhost:8501

---

### Workflow C — Production Live Run

**Prerequisites:** Completed 5-day paper run with Sharpe ≥ 0.5. See Part 7 (paper→live checklist).

**Changes from paper run:**

1. Tighten risk parameters in Streamlit → Config page (see Part 7.2)

2. Fill in `.env.prod`:
   ```dotenv
   IBKR_PORT=7496        # live TWS port
   IBKR_ACCOUNT=U...     # your live account number
   ```

3. Switch IB Gateway to live account login (port 7496)

4. **Terminal 1 — Start prod infrastructure:**
   ```bash
   make prod-infra
   ```

5. **Terminal 2 — Start prod services:**
   ```bash
   source .venv/bin/activate
   honcho start -e .env.prod
   ```

6. **Terminal 3 — Start prod execution:**
   ```bash
   source .venv/bin/activate
   python -m social_trading.services.execution_service --ibkr
   ```

**Open the prod UI:** http://localhost:8502

---

### Workflow D — Simultaneous Test + Production

Run both environments at the same time on the same machine.

**Prerequisites:** Both `.env.test` and `.env.prod` configured. Both TWS instances running
(paper on port 7497, live on port 7496).

**Terminal 1 — Start both Docker stacks:**
```bash
make test-infra && make prod-infra
```

**Terminal 2 — Test services:**
```bash
source .venv/bin/activate
honcho start -e .env.test
# UI at http://localhost:8501
```

**Terminal 3 — Prod services:**
```bash
source .venv/bin/activate
honcho start -e .env.prod
# UI at http://localhost:8502
```

**Terminal 4 — Test execution:**
```bash
python -m social_trading.services.execution_service --ibkr
# reads IBKR_PORT=7497 from .env.test
```

**Terminal 5 — Prod execution:**
```bash
python -m social_trading.services.execution_service --ibkr
# reads IBKR_PORT=7496 from .env.prod
```

See [11-test-prod-environments.md](11-test-prod-environments.md) for full details.

---

---

← [01-environment-setup.md](01-environment-setup.md) &nbsp;|&nbsp; [Index](README.md) &nbsp;|&nbsp; [03-starting-the-system.md](03-starting-the-system.md) →
