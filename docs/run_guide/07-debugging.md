## Part 7 — Debugging

### 7.1 Common Problems

| Symptom | Cause | Fix |
|---------|-------|-----|
| `ConnectionError: 6379` | Redis not running | `make up` |
| `raw_social` stays at 0 | API keys missing or wrong | Check `.env`, verify keys in API portals |
| Signals not generating | Volume z-score too low | Publish a test post (see Workflow A) or lower `spike_zscore_threshold` in Config |
| Execution not trading | Circuit breaker halted | Check `redis-cli get circuit:state`; resume in Streamlit sidebar |
| IBKR connection failed | TWS/Gateway not running | Launch IB Gateway, verify port with `nc -z 127.0.0.1 7497` |
| Streamlit shows no data | Services not started yet | Wait 5 min; check honcho logs for errors |

### 7.2 Inspect Redis State

```bash
# What's in each stream (last entry)?
redis-cli xrevrange raw_social + - COUNT 1
redis-cli xrevrange sentiment_signals + - COUNT 1
redis-cli xrevrange strategy_signals + - COUNT 1

# System config (all parameters)
redis-cli hgetall system:config

# Open positions
redis-cli hgetall positions:open

# Recent trades
redis-cli lrange trades:recent 0 9

# Clear a stream (use carefully — deletes data)
redis-cli del raw_social
```

### 7.3 Re-run a Single Service

If one service is misbehaving, stop it from honcho (`Ctrl+C`), then run it alone:

```bash
source .venv/bin/activate
python -m social_trading.services.nlp_service      # or whichever service
```

You get its logs directly with no other services mixing in.

### 7.4 Crash Recovery

All data is persisted in Docker volumes (`postgres_data`, `redis_data`).
If a service crashes, simply restart honcho or re-run the specific service —
it picks up from where it left off.

If IB Gateway crashes while positions are open:
1. Restart IB Gateway and log in
2. Restart execution service: `python -m social_trading.services.execution_service --ibkr`
3. Check Streamlit → Positions to verify positions reconciled correctly

---

---

← [06-monitoring.md](06-monitoring.md) &nbsp;|&nbsp; [Index](README.md) &nbsp;|&nbsp; [08-paper-trading-checklist.md](08-paper-trading-checklist.md) →
