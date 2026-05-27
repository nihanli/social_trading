## Part 6 — Monitoring System Status

### 6.1 Verify the Pipeline is Flowing

Run these checks 5–10 minutes after startup:

```bash
# Is social data being ingested?
redis-cli xlen raw_social             # expect > 0 within 5 min

# Is sentiment being scored?
redis-cli xlen sentiment_signals      # expect > 0 within 10 min

# Are signals being generated?
redis-cli xlen strategy_signals

# Are signals passing risk screening?
redis-cli xlen selected_signals

# What tickers are being monitored?
redis-cli smembers watchlist:active

# Account state (equity, positions)
redis-cli hgetall account:state

# Circuit breaker state
redis-cli get circuit:state           # expect {"state": "NORMAL"}
```

**Expected healthy pipeline (within 30 minutes of startup):**
- `raw_social` growing → ingest is working
- `sentiment_signals` growing → NLP service is working
- `strategy_signals` growing → signal service is working
- `selected_signals` growing → risk service is approving signals

### 6.2 Service Logs

All logs appear colour-coded in the honcho terminal. Each line is prefixed with the service name:

```
13:01:00 ingest.1    | ...
13:01:01 nlp.1       | ...
```

To capture logs to a file:
```bash
honcho start 2>&1 | tee logs/run_$(date +%Y%m%d).log
```

Docker infrastructure logs:
```bash
docker compose logs -f redis --tail=50
docker compose logs -f postgres --tail=50
```

### 6.3 Grafana & Prometheus Dashboards

```bash
docker compose up -d prometheus grafana
```

| Dashboard | URL | Login |
|-----------|-----|-------|
| Grafana | http://localhost:3000 | admin / your GF_SECURITY_ADMIN_PASSWORD |
| Prometheus | http://localhost:9090 | none |

In Grafana: **Dashboards → Social Trading → Portfolio Overview**

Grafana panels to watch:
- **Equity Curve** — net liquidation value over time
- **System Health** — RED alerts = something is broken
- **Signal Rate** — signals/hour (should not be zero during market hours)
- **Position Count** — open positions (max 5–10 for paper)

### 6.4 API Rate Limit Budget

| Platform | Monthly limit | Our usage | Buffer |
|----------|--------------|-----------|--------|
| X Basic | 10,000 tweets/month | ~3,000 | 3× |
| Reddit | 100 req/min | ~20 req/min | 5× |
| StockTwits | 200 req/hour | ~10 req/hour | 20× |
| yfinance | No hard limit | ~100 req/hour | — |

Monitor X usage at: https://developer.twitter.com/en/portal/usage

---

---

← [05-streamlit-ui.md](05-streamlit-ui.md) &nbsp;|&nbsp; [Index](README.md) &nbsp;|&nbsp; [07-debugging.md](07-debugging.md) →
