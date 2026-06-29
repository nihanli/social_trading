# Plan: Test / Production Environment Split

**Goal:** Run test (IB paper) and production (IB live) environments simultaneously on the same
machine, with complete Docker infrastructure isolation, while preserving the existing
`honcho start` / `Ctrl+C` developer workflow.

**Status:** ✅ Complete — all 6 phases implemented.

---

## Current State

| Resource | Current |
|---|---|
| Docker project | `social_trading` (auto-named from directory) |
| Postgres container | `social_trading-postgres-1` → port `5432` |
| Redis container | `social_trading-redis-1` → port `6379` |
| Postgres DB | `trading` (single database) |
| Redis | DB 0 (3,506 keys) |
| Postgres volumes | `social_trading_postgres_data` |
| Redis volumes | `social_trading_redis_data` |
| Services | Run on host via `honcho start` |
| Config | Single `.env` file |

---

## Target State

| Resource | Test | Production |
|---|---|---|
| Docker project | `social_trading_test` | `social_trading_prod` |
| Postgres container | `social_trading_test-postgres-1` → port `5432` | `social_trading_prod-postgres-1` → port `5433` |
| Redis container | `social_trading_test-redis-1` → port `6379` | `social_trading_prod-redis-1` → port `6380` |
| Postgres DB | `trading_test` | `trading_prod` |
| Redis | DB 0 | DB 0 (isolated by container) |
| Postgres volumes | `social_trading_test_postgres_data` | `social_trading_prod_postgres_data` |
| Redis volumes | `social_trading_test_redis_data` | `social_trading_prod_redis_data` |
| Services | Host via `honcho start -e .env.test` | Host via `honcho start -e .env.prod` |
| Config | `.env.test` | `.env.prod` |
| IB port | `7497` (paper TWS) | `7496` (live TWS) |
| Streamlit UI | `:8501` | `:8502` |

**Test takes over the same host ports as current** (5432, 6379) — zero changes to existing
network config. Prod uses new ports (5433, 6380).

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│  Docker (two isolated project namespaces)                           │
│                                                                     │
│  Project: social_trading_test          Project: social_trading_prod │
│  ┌──────────────────────────┐          ┌────────────────────────┐   │
│  │ postgres  host:5432      │          │ postgres  host:5433     │   │
│  │ redis     host:6379      │          │ redis     host:6380     │   │
│  │ volume: *_test_pg_data   │          │ volume: *_prod_pg_data  │   │
│  │ volume: *_test_redis_data│          │ volume: *_prod_redis_data│  │
│  └──────────────────────────┘          └────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘

  Terminal 1 (test)                       Terminal 2 (prod)
  ─────────────────────────────           ─────────────────────────────
  honcho start -e .env.test               honcho start -e .env.prod
    ingest / nlp / signal / risk            ingest / nlp / signal / risk
    execution → TWS :7497                   execution → TWS :7496
    persistence → trading_test              persistence → trading_prod
    ui → :8501                              ui → :8502
  Ctrl+C stops test services              Ctrl+C stops prod services
```

---

## Phase 1 — Create New Config Files  *(no impact on running system)*

### Files to create

#### `docker-compose.test.yml`
Cloned from `docker-compose.yml` with:
- Project-level name: `social_trading_test`
- Postgres DB env: `POSTGRES_DB=trading_test`
- Postgres port: `5432:5432` (same as current — test takes over)
- Redis port: `6379:6379` (same as current)
- Volume names: `social_trading_test_postgres_data`, `social_trading_test_redis_data`
- Remove `services` profile section (not needed — services run on host)

#### `docker-compose.prod.yml`
Cloned from `docker-compose.yml` with:
- Project-level name: `social_trading_prod`
- Postgres DB env: `POSTGRES_DB=trading_prod`
- Postgres port: `5433:5432`
- Redis port: `6380:6379`
- Volume names: `social_trading_prod_postgres_data`, `social_trading_prod_redis_data`
- Remove `services` profile section

#### `.env.test`
Copy of current `.env` with:
- `DB_NAME=trading_test`
- `REDIS_URL=redis://localhost:6379/0`  *(unchanged)*
- `IBKR_PORT=7497`  *(unchanged — paper)*
- `IBKR_ACCOUNT=DU...`  *(paper account number)*
- `UI_PORT=8501`

> Note: `TRADING_MODE` and `PAPER_INITIAL_CASH` are **not used by any service code** —
> they appear only in `.env.example` as legacy placeholders. Do not include them.

#### `.env.prod`
Copy of `.env.test` with:
- `DB_NAME=trading_prod`
- `REDIS_URL=redis://localhost:6380/0`
- `IBKR_PORT=7496`  *(live TWS)*
- `IBKR_ACCOUNT=U...`  *(live account number)*
- `UI_PORT=8502`

The test vs live distinction is entirely determined by `IBKR_PORT` and `IBKR_ACCOUNT`.

> `.env.prod` is **not committed to git** (already in `.gitignore`).
> Add `.env.test` to `.gitignore` as well (contains API keys).

### `Procfile` change
Parameterize the hardcoded UI port:
```diff
-ui: streamlit run src/social_trading/monitoring/streamlit/app.py --server.port 8501
+ui: streamlit run src/social_trading/monitoring/streamlit/app.py --server.port ${UI_PORT:-8501}
```
`load_dotenv()` inside each service does **not** override env vars already set by honcho's
`-e` flag (Python-dotenv default: `override=False`), so no service code changes are needed.

---

## Phase 2 — Makefile Updates  *(no impact on running system)*

### Targets to add

```makefile
# ── Test environment infrastructure ──────────────────────────────────
test-infra:
    docker compose -p social_trading_test -f docker-compose.test.yml up -d postgres redis

test-infra-down:
    docker compose -p social_trading_test -f docker-compose.test.yml down

test-infra-logs:
    docker compose -p social_trading_test -f docker-compose.test.yml logs -f

# ── Production environment infrastructure ────────────────────────────
prod-infra:
    docker compose -p social_trading_prod -f docker-compose.prod.yml up -d postgres redis

prod-infra-down:
    docker compose -p social_trading_prod -f docker-compose.prod.yml down

prod-infra-logs:
    docker compose -p social_trading_prod -f docker-compose.prod.yml logs -f

# ── Service launchers (honcho shortcuts) ─────────────────────────────
start-test:
    honcho start -e .env.test

start-prod:
    honcho start -e .env.prod

# ── Migrations ───────────────────────────────────────────────────────
migrate-test:
    DB_NAME=trading_test .venv/bin/python migrations/migrate.py

migrate-prod:
    DB_NAME=trading_prod .venv/bin/python migrations/migrate.py

# ── Data sync: promote test reference data to prod ───────────────────
sync-to-prod:
    @./scripts/sync_to_prod.sh
```

### Targets to keep (unchanged)
`install`, `lint`, `format`, `type-check`, `test` (pytest), `test-integration`,
`test-all`, `stop`, `clean`

### Targets to deprecate (kept but noted as legacy)
`up`, `down`, `start`, `migrate`, `services-up`

---

## Phase 3 — Data Migration  *(cutover step — brief downtime)*

This migrates all data from the current `trading` database and Redis DB 0 into the
new test environment containers. Production starts as an empty database.

### 3.1 Stop services

```bash
make stop           # stop honcho services (Ctrl+C if running)
```

### 3.2 Dump current Postgres data

```bash
# Dump entire current trading DB to a file
docker exec social_trading-postgres-1 \
    pg_dump -U trader trading > /tmp/trading_backup.sql
```

### 3.3 Dump current Redis data

```bash
# Save Redis snapshot to disk inside the container
docker exec social_trading-redis-1 redis-cli SAVE

# Copy the RDB file out
docker cp social_trading-redis-1:/data/dump.rdb /tmp/redis_backup.rdb
```

### 3.4 Stop old containers

```bash
# Stop old containers (keeps volumes for safety)
docker compose down
# Volumes social_trading_postgres_data and social_trading_redis_data are preserved
```

### 3.5 Start new test containers

```bash
make test-infra
# Waits for health checks — containers are now:
#   social_trading_test-postgres-1  :5432
#   social_trading_test-redis-1     :6379
```

### 3.6 Create test database and restore Postgres data

```bash
# Create the trading_test database
docker exec social_trading_test-postgres-1 \
    psql -U trader -c "CREATE DATABASE trading_test;"

# Restore the dump
docker exec -i social_trading_test-postgres-1 \
    psql -U trader trading_test < /tmp/trading_backup.sql
```

### 3.7 Restore Redis data

```bash
# Stop Redis briefly to swap RDB file
docker stop social_trading_test-redis-1

# Copy backup RDB into new container's volume
docker cp /tmp/redis_backup.rdb social_trading_test-redis-1:/data/dump.rdb

# Restart Redis (will load from RDB on startup)
docker start social_trading_test-redis-1
```

### 3.8 Run migrations on test DB

```bash
make migrate-test
# Applies any schema changes that may have been added since the backup
```

### 3.9 Verify

```bash
# Verify postgres row counts match original
docker exec social_trading_test-postgres-1 \
    psql -U trader trading_test -c "SELECT COUNT(*) FROM trades; SELECT COUNT(*) FROM signals;"

# Verify Redis key count
docker exec social_trading_test-redis-1 redis-cli info keyspace
# Expected: db0:keys≈3506
```

### 3.10 Update `.env` temporarily for service verification

```bash
cp .env.test .env
make start          # quick smoke test — verify services connect and UI loads
make stop
```

---

## Phase 4 — Production Environment Setup  *(empty — no data migration)*

```bash
# Start prod infrastructure
make prod-infra

# Create prod database
docker exec social_trading_prod-postgres-1 \
    psql -U trader -c "CREATE DATABASE trading_prod;"

# Run migrations (fresh schema)
make migrate-prod

# Seed watchlist (conservative set for live trading)
DB_NAME=trading_prod REDIS_URL=redis://localhost:6380/0 \
    .venv/bin/python scripts/seed_watchlist.py --tickers AAPL MSFT SPY QQQ
```

Prod starts with an **empty trade history** — this is intentional. Reference data (signal
history, sentiment) can optionally be seeded from test using `make sync-to-prod` after
the script is created (see Phase 5).

---

## Phase 5 — Create `scripts/sync_to_prod.sh`  *(optional, run on demand)*

A script to promote reference data from `trading_test` → `trading_prod`. Useful when
you want prod's signal/sentiment history to reflect weeks of test-env learning.

**Tables synced (test → prod):**
- `mention_history` / `social_raw` — historical social mention records
- `sentiment_scores` / `sentiment_aggregates` — scored sentiment history
- `signals` — signal generation history
- `watchlist_candidates` — discovered ticker candidates

**Tables NOT synced (always env-specific):**
- `trades`, `positions`, `account_equity` — trade records are per-env

**Script outline:**
```bash
#!/usr/bin/env bash
# scripts/sync_to_prod.sh — sync reference tables test → prod
# Run: ./scripts/sync_to_prod.sh

set -euo pipefail

TABLES="social_raw sentiment_scores sentiment_aggregates signals"
SRC_CONTAINER=social_trading_test-postgres-1
DST_CONTAINER=social_trading_prod-postgres-1

echo "Syncing reference tables from trading_test → trading_prod"
read -p "This overwrites reference data in prod. Continue? [y/N] " confirm
[[ "$confirm" == "y" ]] || exit 1

for table in $TABLES; do
    echo "  syncing $table..."
    docker exec "$SRC_CONTAINER" \
        pg_dump -U trader trading_test -t "$table" --data-only \
        | docker exec -i "$DST_CONTAINER" \
        psql -U trader trading_prod
done
echo "Done."
```

---

## Phase 6 — Documentation Updates

| File | Changes |
|---|---|
| `docs/run_guide/01-environment-setup.md` | Add `.env.test` / `.env.prod` setup; replace single `.env` instructions |
| `docs/run_guide/02-run-workflows.md` | Add Workflow D (simultaneous test+prod); update Workflow B/C commands |
| `docs/run_guide/03-starting-the-system.md` | Replace `make up` + `honcho start` with env-specific variants |
| `docs/run_guide/04-stopping-the-system.md` | Update stop commands for test/prod |
| `docs/run_guide/10-reference.md` | Update `make` target reference table |
| `docs/run_guide/11-test-prod-environments.md` | Rewrite with final implemented approach (replaces planning doc) |
| `.gitignore` | Ensure `.env.test`, `.env.prod` are listed |

---

## Files Created / Modified Summary

| File | Action |
|---|---|
| `docker-compose.test.yml` | **Create** |
| `docker-compose.prod.yml` | **Create** |
| `.env.test` | **Create** (from current `.env`) |
| `.env.prod` | **Create** (template with prod settings) |
| `Procfile` | **Modify** (1 line: parameterize `UI_PORT`) |
| `Makefile` | **Modify** (add ~12 new targets, deprecate 5 old ones) |
| `scripts/sync_to_prod.sh` | **Create** |
| `docs/run_guide/01-environment-setup.md` | **Update** |
| `docs/run_guide/02-run-workflows.md` | **Update** |
| `docs/run_guide/03-starting-the-system.md` | **Update** |
| `docs/run_guide/04-stopping-the-system.md` | **Update** |
| `docs/run_guide/10-reference.md` | **Update** |
| `docs/run_guide/11-test-prod-environments.md` | **Rewrite** |
| `docker-compose.yml` | **Keep** (legacy, deprecated) |
| **Service code (*.py)** | **No changes** |
| **Migration scripts** | **No changes** |

---

## Rollback Plan

If anything goes wrong during Phase 3 (data migration):
1. The old containers are stopped but **volumes are preserved** (`docker compose down` without `-v`)
2. Restart old containers: `docker compose up -d postgres redis`
3. Verify data is intact: `docker exec social_trading-postgres-1 psql -U trader trading -c "\dt"`
4. Restore `.env` from backup and resume with the original setup

Old containers and volumes remain on the machine until explicitly deleted with
`docker compose down -v` or `docker volume rm`.

---

## Implementation Order

```
Phase 1  → Phase 2  → Phase 3  → Phase 4  → Phase 5  → Phase 6
(files)    (Makefile) (migrate)  (prod DB)  (sync.sh)  (docs)
  ↑ safe     ↑ safe    ↑ brief    ↑ safe     ↑ safe     ↑ safe
  no impact  no impact downtime   no impact  optional   no impact
```

Phases 1 and 2 can be implemented and reviewed without touching the running system.
Phase 3 is the only step with brief downtime (estimated < 5 minutes).
