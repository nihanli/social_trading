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

| Storage | What it holds | Lives in |
|---------|--------------|---------|
| PostgreSQL | Trades, signals, raw posts, config snapshots | `postgres_data` Docker volume |
| Redis | Stream data, watchlist, live config, positions | `redis_data` Docker volume |

Both volumes survive `make down`. Only `docker compose down -v` deletes them.

### 10.4 Ports Reference

| Service | Port | Notes |
|---------|------|-------|
| Streamlit UI | 8501 | http://localhost:8501 |
| Grafana | 3000 | admin login required |
| Prometheus | 9090 | no login |
| Redis | 6379 | internal, not for browser |
| Postgres | 5432 | internal, not for browser |
| TWS paper | 7497 | IBKR API socket |
| Gateway paper | 4002 | IBKR API socket |
| Gateway live | 4001 | IBKR API socket |

---

← [09-live-trading.md](09-live-trading.md) &nbsp;|&nbsp; [Index](README.md)
