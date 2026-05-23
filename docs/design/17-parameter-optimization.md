## 17. Parameter Optimization & Continuous Improvement

The system closes the loop between live trading and parameter tuning through a structured
feedback pipeline: every session's config is saved alongside its performance metrics, the
Streamlit Optimize page analyses patterns across runs, and walk-forward grid search
validates changes against recent historical data before they are applied.

---

### 17a. The Feedback Loop

```
                  ┌─────────────────────────────────┐
                  │   Post-market (4:00–5:00 PM ET) │
                  └─────────────┬───────────────────┘
                                │
          ┌─────────────────────▼────────────────────────┐
          │  SystemConfig.save_run_snapshot(metrics)      │
          │  Writes to config_runs table (PostgreSQL)      │
          │  Fields: config_snapshot (JSONB) + P&L,        │
          │  win_rate, Sharpe, exit breakdown, signal stats│
          └─────────────────────┬────────────────────────┘
                                │
          ┌─────────────────────▼────────────────────────┐
          │  Streamlit → Optimize page (6_optimize.py)    │
          │                                               │
          │  Tab 1: Run History      — compare sessions   │
          │  Tab 2: Sensitivity      — param vs metric    │
          │  Tab 3: Auto-Suggestions — rule-based alerts  │
          │  Tab 4: Grid Search      — walk-forward opt.  │
          └─────────────────────┬────────────────────────┘
                                │
          ┌─────────────────────▼────────────────────────┐
          │  Save updated SystemConfig to Redis           │
          │  All services pick up new params next cycle   │
          └──────────────────────────────────────────────┘
```

---

### 17b. EOD Snapshot — When and How to Trigger

The snapshot should be saved automatically at the end of each trading session. Add this
to the execution engine's end-of-day flush routine:

```python
import psycopg2, redis
from config.system_config import SystemConfig
from utils.db import query_scalar

rc  = redis.Redis()
cfg = SystemConfig.load(rc)

def save_eod_snapshot(mode: str = "paper") -> None:
    """
    Gather today's performance metrics from PostgreSQL and persist the
    config + metrics pair. Called by the EOD flush at ~4:00 PM ET.
    """
    today_metrics = _gather_today_metrics(mode)
    cfg.save_run_snapshot(today_metrics, mode=mode)

def _gather_today_metrics(mode: str) -> dict:
    from utils.db import query   # pandas read_sql helper
    import pandas as pd

    trades = query(f"""
        SELECT net_pnl, exit_reason,
               EXTRACT(EPOCH FROM (closed_at - opened_at)) / 3600 AS hold_hrs
        FROM trades
        WHERE closed_at::date = CURRENT_DATE AND mode = '{mode}'
          AND closed_at IS NOT NULL
    """)
    signals = query(f"""
        SELECT confidence, executed
        FROM signals
        WHERE generated_at::date = CURRENT_DATE
    """)

    if trades.empty:
        return {}

    pnls = trades["net_pnl"]
    gross_profit = pnls[pnls > 0].sum()
    gross_loss   = pnls[pnls < 0].sum()

    exit_counts = trades["exit_reason"].value_counts().to_dict()

    daily_returns = pnls / pnls.abs().sum()   # simplified daily return series
    sharpe = (daily_returns.mean() / (daily_returns.std() + 1e-9)) * (252 ** 0.5)

    return {
        "total_pnl":                float(pnls.sum()),
        "total_trades":             len(trades),
        "win_count":                int((pnls > 0).sum()),
        "win_rate":                 float((pnls > 0).mean()),
        "sharpe_ratio":             float(sharpe),
        "max_drawdown":             float((pnls.cumsum() - pnls.cumsum().cummax()).min()),
        "avg_hold_hours":           float(trades["hold_hrs"].mean()),
        "profit_factor":            float(gross_profit / abs(gross_loss)) if gross_loss else None,
        "exits_take_profit":        int(exit_counts.get("TAKE_PROFIT", 0)),
        "exits_time_stop":          int(exit_counts.get("TIME_STOP", 0)),
        "exits_atr_stop":           int(exit_counts.get("ATR_STOP", 0)),
        "exits_trailing_stop":      int(exit_counts.get("TRAILING_STOP", 0)),
        "exits_sentiment_reversal": int(exit_counts.get("SENTIMENT_REVERSAL", 0)),
        "exits_mention_decay":      int(exit_counts.get("MENTION_DECAY", 0)),
        "exits_manual":             int(exit_counts.get("MANUAL", 0)),
        "signals_generated":        len(signals),
        "signals_executed":         int(signals["executed"].sum()),
        "avg_signal_quality":       float(signals["confidence"].mean()) if not signals.empty else None,
    }
```

---

### 17c. Auto-Suggestion Rules

The Optimize page applies these rules to the last N sessions. Each rule maps an observed
symptom to a specific parameter change.

| Symptom | Threshold | Suggestion | Parameter |
|---------|-----------|-----------|-----------|
| Low win rate | < 45% | Raise quality gate | ↑ `signal_quality_threshold` |
| Time stop dominates exits | > 50% of exits | Signal decays before target — lower take-profit or hold time | ↓ `take_profit_pct` or `max_hold_hours` |
| ATR stop dominates | > 40% of exits | Stops too tight for instrument volatility | ↑ `atr_multiplier` |
| Sentiment reversal common | > 25% of exits | Signals reversing early — tighten entry bar | ↑ `signal_quality_threshold` or ↑ `signal_reversal_threshold` |
| Negative Sharpe | < 0.0 avg | Strategy not working — halt live, review all params | Switch to paper |
| Very low signal execution rate | < 10% of generated | Circuit breaker firing too often | Review `loss_limit_daily` / `drawdown_halt` |

These are starting heuristics. The sensitivity analysis (Tab 2) will surface more nuanced
relationships once 10+ sessions have been recorded.

---

### 17d. Walk-Forward Grid Search — Design Principles

The grid search in Tab 4 of the Optimize page uses the **last N days of executed trades**
from the database as the test set. This is intentionally simple — it is **not** a full
vectorbt backtest (see §12 for that). It is a fast sensitivity check: *given the signals
we actually fired recently, which parameter settings would have produced better outcomes?*

Key safeguards built into the grid search:

| Safeguard | Implementation |
|-----------|----------------|
| No look-ahead | Only uses `signals` with `executed=true` and closes prices after signal timestamp |
| Transaction cost | 20bps round-trip deducted from each trade |
| Minimum trades filter | Combinations with < 5 trades are excluded |
| Combination cap | UI prevents running > 500 combinations at once |
| Applies to paper only | One-click apply goes to config; if in live mode, trader must confirm |

**When to run grid search:**
- After accumulating 10+ sessions of paper trading data
- When auto-suggestions point to the same parameter repeatedly
- Before switching from paper to live mode

**What grid search cannot do:**
- Test parameters that affect signal *generation* (e.g. `spike_zscore_threshold`) — those
  determine which signals exist in the dataset. Changing them requires a full vectorbt
  backtest with raw social data, not just re-simulating existing executions.

---

### 17e. Progression Recommendation

| Phase | Sessions | Actions |
|-------|----------|---------|
| Early paper trading | 1–9 | Trust defaults; collect data; review auto-suggestions manually |
| Mid paper trading | 10–29 | Run sensitivity analysis; apply 1–2 auto-suggestions at a time |
| Late paper trading | 30+ | Run grid search; walk-forward validate best config; compare Sharpe |
| Pre-live | — | Config must show Sharpe > 0.5 and win rate > 48% over last 20 sessions |
| Live | Ongoing | EOD snapshots continue; grid search monthly or after drawdown events |

---

*[⬆ Back to main index](README.md)*
