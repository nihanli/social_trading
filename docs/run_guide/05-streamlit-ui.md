## Part 5 — Using the Streamlit UI

Open **http://localhost:8501**

The UI has a persistent **sidebar** and **6 pages** in the left navigation.

---

### Sidebar — System Controls (always visible)

| Element | What it does |
|---------|-------------|
| Status banner | `NORMAL` (green) or `HALTED` (orange) |
| **🛑 Halt New Positions** | Stops execution from opening new trades. Existing positions unaffected. |
| **▶ Resume Trading** | Re-enables new trades after a halt. Only shown when halted. |
| **Emergency Actions** ▾ | Reveals **Close ALL Positions** — closes every open position at market immediately. Irreversible. |

---

### Page 1 — Main Dashboard

At-a-glance system overview. Check this first thing every session.

| Section | What to look at |
|---------|----------------|
| **KPI row** | Daily P&L %, Circuit Breaker state, VIX level |
| **Equity Curve** | Should trend upward. Steepening downward slope = review immediately |
| **Open Positions table** | Ticker, direction, qty, entry, current price, unrealised P&L, stop/TP. Each row has a **Close [TICKER]** button |
| **Recent Signals** | Last 20 signals that passed risk screening. Check quality scores |
| **Sentiment Heatmap** | Green = bullish, Red = bearish, Grey = no data. Quick cross-ticker view |
| **Recent Closed Trades** | Last 20 completed trades with P&L |

---

### Page 2 — Open Positions

Detailed position management.

- Full position detail: entry time, hours held, cost basis, unrealised P&L, stop price, TP price, high-water mark
- **Close [TICKER]** button per row — sends `CLOSE_TICKER` command to execution service
- **Close ALL Positions** emergency button (same as sidebar)

**When to use:** Check at market open (9:30 AM ET) and before market close (3:45 PM ET).

---

### Page 3 — Signal Feed

Live view of signals passing through the pipeline.

- Signal count per ticker (last 24h), quality score histogram, volume z-score over time
- Full searchable signals table: ticker, direction, quality, sentiment, volume z-score, momentum, convergence, timestamp

**What healthy signals look like:**
- Quality scores distributed above 0.6 threshold
- Volume z-scores occasionally spiking to 2–4 (not always near 0)
- Mix of tickers (not one ticker dominating)

**Warning signs:**
- All scores near 0 → no social momentum detected; check API keys
- One ticker getting 90% of signals → possible bot activity or API issue

---

### Page 4 — Trade Analytics

Historical performance of all closed trades.

- Cumulative P&L, daily P&L bar chart
- **P&L by Exit Reason:** TAKE_PROFIT / STOP_LOSS / EMERGENCY / TIME_STOP / UI:CLOSE_ALL
- P&L by ticker, full trade history with date filters

**Healthy pattern:** TAKE_PROFIT exits outnumber STOP_LOSS. Win rate ≥ 45%.  
**Warning:** High EMERGENCY or STOP_LOSS % → risk parameters too loose.

---

### Page 5 — Sentiment Heatmap

Deep-dive into raw social sentiment.

- **Look-back slider (1–24h):** Use 4h for intraday momentum, 24h for multi-day trends
- **Sentiment heatmap:** Rows = tickers, cells = sentiment by time bucket
- **Posts by Source pie chart:** Verify all enabled sources are contributing
- **Sentiment Label Distribution:** Should not be 90%+ neutral (VADER thresholds too wide)

---

### Page 6 — System Configuration

Live parameter editor. Changes take effect within 60 seconds — no restart needed.

#### Watchlist
- Pin/unpin tickers (pinned = never auto-expires from watchlist)
- Default: auto-adds tickers when social mention volume spikes

#### Signal Quality
| Parameter | Default | Effect |
|-----------|---------|--------|
| Signal quality threshold | 0.60 | Raise to reduce noise / false signals |
| Volume z-score weight | 0.30 | Importance of mention spike |
| Sentiment weight | 0.25 | Importance of polarity |
| Proactivity weight | 0.20 | Bonus for pre-price signals |
| Price momentum weight | 0.15 | Corroborating price movement |
| Cross-platform weight | 0.10 | Multi-source agreement bonus |

#### Position Sizing
| Parameter | Default | Live recommendation |
|-----------|---------|---------------------|
| Max position % of NLV | 2% | **0.5%** |
| Half-Kelly fraction | 0.5 | **0.25** |
| Max social allocation | 20% | 10% |

#### Exit Rules
| Parameter | Default | Effect |
|-----------|---------|--------|
| Take profit % | 6% | Close when unrealised gain = 6% |
| Trailing stop % | 2% | Close if retraces 2% from high-water |
| ATR multiplier | 2.0 | Stop = entry ± 2×ATR |
| Max hold hours | 48 | Hard time stop |

#### Risk & Circuit Breakers
| Parameter | Default | Triggers |
|-----------|---------|---------|
| Single trade loss limit | 1% | Emergency close of that position |
| Daily loss limit | 3% | Halt new trades for rest of day |
| Weekly loss limit | 7% | Reduce all sizes 50% |
| Drawdown halt | 15% | Full halt until manual resume |
| VIX crisis (>40) | 40 | 0% position size — no trading |

**Save Configuration** — writes to Redis; all services reload on next cycle.  
**Reset to Defaults** — reverts all parameters to code defaults.

---

### Page 7 — Parameter Optimization

Four tabs for tuning parameters based on historical trades:

| Tab | Purpose |
|-----|---------|
| **Run History** | Compare parameter sets across all past sessions |
| **Sensitivity Analysis** | Scatter plot of any parameter vs any metric (Pearson correlation) |
| **Auto-Suggestions** | Median values from your N best sessions vs current config |
| **Grid Search** | Brute-force test parameter combinations against historical trades |

> ⚠️ Grid search is in-sample only. Always re-test any changes for ≥2 days in
> paper mode before going live.

---

---

← [04-stopping-the-system.md](04-stopping-the-system.md) &nbsp;|&nbsp; [Index](README.md) &nbsp;|&nbsp; [06-monitoring.md](06-monitoring.md) →
