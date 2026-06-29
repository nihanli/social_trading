## Part 10 — Reference

### 10.1 Network Considerations

**Residential / home connection:** Works fine for all APIs and IBKR.

**Remote server (VPS):**
- Use US-East region (New York/Virginia) for lowest latency to US market data
- Firewall: open ports 8501 (Streamlit), 3000 (Grafana), 9090 (Prometheus); keep 7497/4002 closed
- Access UI via SSH tunnel:
  ```bash
  ssh -L 8501:localhost:8501 -L 3000:localhost:3000 user@your-server-ip
  ```

### 10.2 Market Hours

The system runs 24/7 but IBKR only executes orders during market hours:
**9:30 AM – 4:00 PM ET, Monday–Friday**

Signals generated outside market hours accumulate in the `selected_signals`
Redis stream and are consumed when the market opens — IBKR will reject them
if the market is still closed (expected, non-fatal behaviour).

### 10.3 Data Persistence

| Storage | What it holds | Test volume | Prod volume |
|---------|--------------|-------------|-------------|
| PostgreSQL | Trades, signals, raw posts, config snapshots | `social_trading_test_postgres_data` | `social_trading_prod_postgres_data` |
| Redis | Stream data, watchlist, live config, positions | `social_trading_test_redis_data` | `social_trading_prod_redis_data` |

Volumes survive `make test-infra-down` / `make prod-infra-down`. Only `docker compose ... down -v` deletes them.

### 10.4 Ports Reference

| Service | Test | Prod | Notes |
|---------|------|------|-------|
| Streamlit UI | 8501 | 8502 | http://localhost:850x |
| Grafana | 3000 | 3001 | admin login required |
| Prometheus | 9090 | 9091 | no login |
| Redis | 6379 | 6380 | internal |
| Postgres | 5432 | 5433 | internal |
| TWS paper | 7497 | — | IBKR API socket |
| TWS live | — | 7496 | IBKR API socket |
| Gateway paper | 4002 | — | IBKR API socket |
| Gateway live | — | 4001 | IBKR API socket |

### 10.5 Make Target Reference

| Target | Description |
|--------|-------------|
| `make test-infra` | Start test Postgres (:5432) + Redis (:6379) |
| `make test-infra-down` | Stop test containers (keep volumes) |
| `make test-infra-logs` | Tail test container logs |
| `make prod-infra` | Start prod Postgres (:5433) + Redis (:6380) |
| `make prod-infra-down` | Stop prod containers (keep volumes) |
| `make prod-infra-logs` | Tail prod container logs |
| `make start-test` | `honcho start -e .env.test` |
| `make start-prod` | `honcho start -e .env.prod` |
| `make migrate-test` | Run migrations on `trading_test` |
| `make migrate-prod` | Run migrations on `trading_prod` |
| `make sync-to-prod` | Sync reference tables from test → prod |
| `make test` | Run unit tests |
| `make lint` | Run linter |

---

← [09-live-trading.md](09-live-trading.md) &nbsp;|&nbsp; [Index](README.md)
