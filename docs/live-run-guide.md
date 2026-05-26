# Live Paper-Trading Run — Complete Setup & Operations Guide

## Overview

This guide covers the full setup for a **5-day paper trading dry run** — the
final gate before going live with real money. The run uses Interactive Brokers
paper account (no real trades), real social media APIs (real data), and real
market data via yfinance.

**Minimum hardware:** 4-core CPU, 8 GB RAM, 20 GB disk, stable internet  
**OS:** macOS or Ubuntu 22.04+ (both supported)  
**Time to first trade signal:** approximately 30–60 minutes after all APIs are activated

---

## Part 1 — Social Media API Setup

### 1.1 X (Twitter) API

You need **X API v2 Basic or Pro tier** ($100/month Basic, $5,000/month Pro).
The free tier does NOT provide streaming or search endpoints needed here.

**Steps:**

1. Go to https://developer.twitter.com/en/portal/dashboard
2. Click **+ Create Project** → name it `social-trading`
3. Create an App inside the project → choose **Production** environment
4. Under **Keys and Tokens**, generate:
   - `Bearer Token` → maps to `X_BEARER_TOKEN`
   - `API Key & Secret` → maps to `X_API_KEY` / `X_API_SECRET`
   - `Access Token & Secret` → maps to `X_ACCESS_TOKEN` / `X_ACCESS_SECRET`
5. Under **App Settings → Authentication settings**, enable **Read** permissions
6. Under **Product Settings**, confirm `Basic` tier (or higher) is active

**Permissions needed:**  `Read` on public tweets is sufficient.

**Rate limits at Basic tier:**
- Search (recent): 10,000 tweets/month, 1 request/second
- The ingest service respects `x-rate-limit-reset` headers automatically

> ⚠️ If you only want to test the pipeline without X costs, set
> `X_BEARER_TOKEN=` empty — the ingest service will skip Twitter and use
> Reddit + StockTwits only.

---

### 1.2 Reddit API

Reddit API is **free** with reasonable rate limits.

**Steps:**

1. Log in at https://www.reddit.com/prefs/apps
2. Click **create another app** (bottom of page)
3. Fill in:
   - **name:** `social-trading`
   - **type:** select `script`
   - **redirect uri:** `http://localhost:8080`
   - **description:** anything
4. Click **create app**
5. Copy values:
   - The string under the app name (14-char code) → `REDDIT_CLIENT_ID`
   - The `secret` field → `REDDIT_CLIENT_SECRET`
6. Set `REDDIT_USER_AGENT` to a descriptive string, e.g.:
   `social-trading-bot/0.1 by u/YourUsername`

**Rate limits:** 100 requests/minute on OAuth (well within ingest cadence)

**Default subreddits monitored:** `wallstreetbets+stocks+options+investing`  
You can override by modifying `DEFAULT_SUBREDDITS` in `src/social_trading/ingest/sources/reddit.py`.

---

### 1.3 StockTwits API

StockTwits offers a **free public API** for reading the stream.

**Steps:**

1. Register at https://api.stocktwits.com/developers/apps/new
2. Create a new application
3. Copy the **OAuth Token** → `STOCKTWITS_TOKEN`

> StockTwits token is optional — the ingest service gracefully skips it if
> the env var is empty.

---

## Part 2 — Interactive Brokers Setup (Paper Trading)

### 2.1 Create a Paper Trading Account

1. Log in to https://www.interactivebrokers.com
2. Navigate to **Account Management → Paper Trading Account** (or create one at signup)
3. Fund the paper account with a simulated amount — recommend **$100,000**

### 2.2 Install TWS or IB Gateway

Two options; **IB Gateway is preferred** (headless, lower resource usage):

**Option A — IB Gateway (recommended for servers):**
1. Download from: https://www.interactivebrokers.com/en/trading/ibgateway-latest.php
2. Install and launch
3. Log in with your **paper trading** credentials (separate login from live)
4. Navigate to **Configure → Settings → API**:
   - ☑ Enable ActiveX and Socket Clients
   - ☑ Allow connections from localhost only
   - Socket port: **4002** (paper) or **4001** (live)
   - ☑ Read-Only API: **OFF** (must be off to place orders)
   - Master Client ID: leave as **0**

**Option B — Trader Workstation (TWS):**
1. Download from: https://www.interactivebrokers.com/en/trading/tws.php
2. Install and launch with **paper trading** login
3. Navigate to **Edit → Global Configuration → API → Settings**:
   - ☑ Enable ActiveX and Socket Clients
   - Socket port: **7497** (paper TWS default)
   - ☑ Allow connections from localhost only

### 2.3 Configure Port in .env

```
IBKR_PORT=4002          # IB Gateway paper
# OR
IBKR_PORT=7497          # TWS paper
```

> ⚠️ The execution service uses `network_mode: host` in docker-compose so it
> can reach `127.0.0.1:7497/4002`. Do NOT change this.

---

## Part 3 — Environment Setup

### 3.1 Machine Prerequisites

```bash
# macOS
brew install docker docker-compose git python@3.14

# Ubuntu 22.04
sudo apt update && sudo apt install -y docker.io docker-compose-v2 git python3.14 python3.14-venv
sudo usermod -aG docker $USER   # then re-login
```

### 3.2 Clone and Install

```bash
git clone https://github.com/nihanli/social_trading.git
cd social_trading
python3.14 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### 3.3 Create .env File

Copy the example and fill in your values:

```bash
cp .env.example .env
nano .env          # or use your editor
```

Complete `.env` for paper trading run:

```dotenv
# ── Database ────────────────────────────────────────────────────────────────
DB_HOST=localhost
DB_PORT=5432
DB_NAME=trading
DB_USER=trader
DB_PASSWORD=changeme          # change this in production

# ── Redis ───────────────────────────────────────────────────────────────────
REDIS_URL=redis://localhost:6379/0

# ── X (Twitter) API ─────────────────────────────────────────────────────────
X_BEARER_TOKEN=AAAAAAAAAAAAAAAAAAAAAxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
X_API_KEY=your_api_key_here
X_API_SECRET=your_api_secret_here
X_ACCESS_TOKEN=your_access_token_here
X_ACCESS_SECRET=your_access_secret_here

# ── Reddit API ───────────────────────────────────────────────────────────────
REDDIT_CLIENT_ID=abc123xyz456
REDDIT_CLIENT_SECRET=your_secret_here
REDDIT_USER_AGENT=social-trading-bot/0.1 by u/YourUsername

# ── StockTwits ────────────────────────────────────────────────────────────────
STOCKTWITS_TOKEN=your_token_here

# ── Interactive Brokers ──────────────────────────────────────────────────────
IBKR_HOST=127.0.0.1
IBKR_PORT=7497              # 7497=TWS paper, 4002=Gateway paper
IBKR_CLIENT_ID=10
IBKR_PAPER=true

# ── System ───────────────────────────────────────────────────────────────────
LOG_LEVEL=INFO
TRADING_MODE=paper
PAPER_INITIAL_CASH=100000

# ── Grafana ───────────────────────────────────────────────────────────────────
GF_SECURITY_ADMIN_PASSWORD=your_secure_grafana_password
```

> ⚠️ Never commit `.env` to git. It is already in `.gitignore`.

---

## Part 4 — Command Sequence (Launch Order)

Run these in order. Each step depends on the previous being healthy.

### Step 1 — Start Infrastructure

```bash
# Start postgres and redis
make up
# OR equivalently:
docker compose up -d postgres redis

# Verify both are healthy (wait ~10 seconds)
docker compose ps
```

Expected output: both show `(healthy)`.

### Step 2 — Run Database Migrations

```bash
make migrate
# OR:
python migrations/migrate.py
```

Expected: `Migrations applied: 001_initial_schema.sql, 002_config_runs.sql`

### Step 3 — Seed the Watchlist

```bash
source .venv/bin/activate
python scripts/seed_watchlist.py

# Optional: override default tickers
python scripts/seed_watchlist.py --tickers AAPL TSLA NVDA AMD MSFT SPY QQQ
```

Verify: `redis-cli smembers watchlist:active` should list your tickers.

### Step 4 — Start IB Gateway / TWS

Launch IB Gateway (or TWS) manually on the host machine and log in with
your paper trading credentials. Confirm the API socket is listening:

```bash
# Verify TWS/Gateway is reachable
nc -z 127.0.0.1 7497 && echo "TWS reachable" || echo "TWS NOT reachable"
# OR for Gateway:
nc -z 127.0.0.1 4002 && echo "Gateway reachable" || echo "Gateway NOT reachable"
```

### Step 5 — Start Application Services

```bash
# Start all 9 services (ingest, nlp, signal, risk, execution, streamlit, prometheus, grafana)
make services-up
# OR:
docker compose up -d

# For the first run, rebuild images first:
docker compose build --no-cache
docker compose up -d
```

Wait ~30 seconds, then verify all containers are running:

```bash
docker compose ps
```

All 9 services should show `Up` or `(healthy)`.

### Step 6 — Start Execution in IBKR Mode

The execution service starts in paper mode by default. To connect to IBKR:

```bash
# Stop the default execution container
docker compose stop execution

# Run execution service directly on host (required for IBKR localhost access)
source .venv/bin/activate
python -m social_trading.services.execution_service --ibkr
```

Leave this terminal open — this is the live process.

> Why not Docker for execution? Docker networking isolates containers from
> `localhost:7497`. Running directly on host bypasses this.
> Alternatively, keep `network_mode: host` in docker-compose (already configured)
> and it will work on Linux. On macOS, run directly.

### Step 7 — Open Monitoring Dashboards

| Dashboard | URL | Credentials |
|-----------|-----|-------------|
| Streamlit UI | http://localhost:8501 | none |
| Grafana | http://localhost:3000 | admin / (your GF_SECURITY_ADMIN_PASSWORD) |
| Prometheus | http://localhost:9090 | none |

In Grafana, navigate to **Dashboards → Social Trading → Portfolio Overview**.

---

## Part 5 — Verifying the System is Working

### 5.1 Check Data is Flowing (within 5 minutes of startup)

```bash
# Posts being ingested?
docker logs social_trading-ingest-1 --tail 20

# Redis streams have data?
redis-cli xlen raw_social
redis-cli xlen sentiment_signals
redis-cli xlen strategy_signals
redis-cli xlen selected_signals

# Active tickers in watchlist?
redis-cli smembers watchlist:active
```

Expected: `raw_social` should have entries within 2–5 minutes.

### 5.2 Check Sentiment is Being Scored

```bash
docker logs social_trading-nlp-1 --tail 20
redis-cli xlen sentiment_signals
```

### 5.3 Check Signals are Being Generated

```bash
docker logs social_trading-signal-1 --tail 20
redis-cli xlen strategy_signals
```

### 5.4 Check Risk Service is Approving Signals

```bash
docker logs social_trading-risk-1 --tail 20
redis-cli xlen selected_signals
```

### 5.5 Check a Trade was Placed

```bash
# Paper positions (last 10 trades)
redis-cli lrange trades:recent 0 9

# Account state
redis-cli hgetall account:state
```

---

## Part 6 — 5-Day Paper Trading Run Checklist

### Day 1 (Monday) — Baseline

- [ ] All services green in `docker compose ps`
- [ ] At least 50 posts per hour in `raw_social` stream
- [ ] At least one signal generated by end of day
- [ ] IBKR paper account shows correct equity ($100,000)
- [ ] Grafana Equity Curve panel shows a flat line (no trades yet may be OK)
- [ ] Check circuit breaker state: `redis-cli get circuit:state` → `{"state": "NORMAL"}`

### Day 2–4 — Active Monitoring (twice daily: 9:30 AM and 3:30 PM ET)

Each session, verify:
- [ ] `redis-cli hgetall account:state` — equity moving, no runaway losses
- [ ] Streamlit → Positions page — max 5–10 open positions
- [ ] Streamlit → Signals page — signal quality distribution looks reasonable (not all 0 or 1)
- [ ] Grafana → System Health — no RED alerts firing
- [ ] Log check: `docker logs social_trading-execution-1 --tail 50`

### Day 5 (Friday) — Evaluation Gate

Run the Sharpe > 0.5 gate calculation:

```bash
source .venv/bin/activate
python - <<'EOF'
import psycopg2, os
from dotenv import load_dotenv
load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST", "localhost"),
    dbname=os.getenv("DB_NAME", "trading"),
    user=os.getenv("DB_USER", "trader"),
    password=os.getenv("DB_PASSWORD", "changeme"),
)
cur = conn.cursor()
cur.execute("""
    SELECT
        COUNT(*)                          AS trades,
        AVG(pnl_pct)                      AS mean_return,
        STDDEV(pnl_pct)                   AS std_return,
        AVG(pnl_pct) / NULLIF(STDDEV(pnl_pct), 0) * SQRT(252) AS annualised_sharpe,
        SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END)::float / COUNT(*) AS win_rate
    FROM trades
    WHERE closed_at >= NOW() - INTERVAL '5 days'
""")
row = cur.fetchone()
print(f"Trades:    {row[0]}")
print(f"Mean ret:  {row[1]:.4%}")
print(f"Std ret:   {row[2]:.4%}")
print(f"Sharpe:    {row[3]:.2f}")
print(f"Win rate:  {row[4]:.1%}")
conn.close()
EOF
```

**Gate: Sharpe ≥ 0.5 before proceeding to live.**

---

## Part 7 — Streamlit UI Guide

Open **http://localhost:8501** in your browser. The UI has a sidebar for
system control and 6 pages accessible from the left navigation panel.

---

### 7.1 Main Dashboard (home page)

The home page is your primary at-a-glance view.

**Sidebar — System Controls** (always visible on every page):

| Control | What it does |
|---------|-------------|
| Status banner | Shows `NORMAL` (green) or `HALTED` (orange) trading state |
| **Halt New Trades** button | Stops the execution service from opening any new positions. Existing positions are unaffected. Use this during volatile conditions or when you want to observe without trading. |
| **Resume Trading** button | Re-enables new position opening after a halt. Only visible when system is halted. |
| **Emergency Actions** expander | Reveals the `Close ALL Positions` button. Sends `CLOSE_ALL` to the execution engine — all open positions are closed at market price immediately. **Irreversible.** |

**KPI Row** (top of main content):

| Metric | Description |
|--------|-------------|
| Daily P&L % | Today's unrealised + realised P&L as % of equity |
| Circuit Breaker | Current state: `NORMAL`, `REDUCED_50`, `DAILY_HALT`, or `FULL_HALT` |
| VIX | Current VIX level (from yfinance). Drives position sizing scalar. |

**Equity Curve panel:** Running net liquidation value since start. A downward slope that steepens should trigger manual review.

**Open Positions table:** Ticker, direction, quantity, entry price, current price, unrealised P&L, stop loss, take profit. Each row has a **Close [TICKER]** button for manual closure.

**Recent Signals table:** Last 20 signals that passed risk screening. Columns: ticker, direction, quality score, sentiment score, volume z-score, timestamp.

**Sentiment Heatmap:** Colour-coded grid of top tickers vs last-hour sentiment — green = bullish, red = bearish, grey = no data.

**Recent Closed Trades table:** Last 20 completed trades with P&L.

---

### 7.2 Page 1 — Open Positions

Detailed view of every open position.

- **Positions table:** Full detail including entry time, hours held, cost basis, unrealised P&L, stop loss price, take profit price, high-water mark.
- **Close Individual Positions:** One button per ticker. Sends `CLOSE_TICKER` command with the ticker name to the execution engine. The position is closed at the next market price.
- **Emergency Actions expander:** `Close ALL Positions Now` — same as sidebar emergency button, available here for convenience.

**When to use:** Check this page at market open (9:30 AM ET) and before market close (3:45 PM ET) each day during the paper run.

---

### 7.3 Page 2 — Signal Feed

Live feed of all signals passing through the pipeline.

- **Top section:** Three bar/line charts showing:
  - Signal count per ticker (last 24h)
  - Quality score distribution histogram
  - Volume z-score over time
- **All Signals table:** Full searchable/filterable table of recent signals. Columns: ticker, direction, quality, sentiment, volume z-score, momentum, convergence, post count, timestamp.

**What to look for:**
- Quality scores clustered above 0.6 (the threshold) = system is discriminating well
- Volume z-scores consistently near 0 = no real momentum being detected (consider lowering spike threshold in Config)
- One ticker dominating all signals = potential over-weighting, check Config → Signal Quality → Factor Weights

---

### 7.4 Page 3 — Trade Analytics

Historical performance breakdown of all closed trades.

- **Mode toggle:** Switch between `paper` and `live` views (for when you have both)
- **Cumulative P&L chart:** Running total of realised P&L over time
- **Daily P&L bar chart:** Profit/loss by calendar day — easy to spot losing days
- **P&L by Exit Reason:** Breakdown of TAKE_PROFIT / STOP_LOSS / EMERGENCY / TIME_STOP / UI:CLOSE_ALL exits. High EMERGENCY or STOP_LOSS % = risk parameters too loose.
- **P&L by Ticker:** Which tickers are contributing / detracting most
- **All Closed Trades table:** Full history with selectbox to filter by last 7 / 30 / 90 / 365 days

**What to look for during paper run:**
- Win rate above 45% is acceptable for a momentum strategy
- Average win should be larger than average loss (positive expectancy)
- TAKE_PROFIT exits should outnumber EMERGENCY exits
- If STOP_LOSS exits dominate, consider widening stop (increase `atr_multiplier` in Config)

---

### 7.5 Page 4 — Sentiment Heatmap

Deep-dive into the raw sentiment signals feeding the strategy.

- **Look-back window slider (1–24 hours):** Adjusts how far back sentiment data is aggregated. Use 4h for intraday momentum; use 24h to spot multi-day trends.
- **Sentiment heatmap:** Rows = tickers, cells = sentiment score by time bucket. Deep green = strong bullish, deep red = strong bearish.
- **Sentiment Over Time chart:** Line chart of sentiment score per top ticker over the selected window. Look for tickers where sentiment crosses from negative to positive (momentum entry signal).
- **Posts by Source pie chart:** Distribution of posts across Twitter, Reddit, StockTwits. If one source disappears, check API keys and logs.
- **Sentiment Label Distribution bar chart:** Count of positive / neutral / negative posts. Should not be 90%+ neutral (suggests VADER thresholds too wide).

---

### 7.6 Page 5 — System Configuration

Live parameter editor. **Changes take effect on the next service cycle** (within 60 seconds) — no restarts needed. All values are saved to Redis.

#### Section 1: Watchlist Management
- **Active Watchlist list:** All tickers currently being monitored
- **Pin Ticker input + button:** Adds a ticker permanently (never auto-expires). Use for core holdings like AAPL, TSLA.
- **Unpin Ticker input + button:** Removes a ticker from the permanent list (it may still stay if organically trending)

#### Section 2: Discovery & Spike Detection
| Parameter | Default | Effect |
|-----------|---------|--------|
| Spike Z-score threshold | 2.0 | Minimum mention volume z-score to add a ticker to watchlist. Lower = more tickers monitored. |
| Mention window (minutes) | 60 | Rolling window for computing mention volume. |
| X search max results | 100 | Posts fetched per API call. Higher = more data, more API cost. |
| Watchlist stale hours | 48 | Remove ticker from watchlist if no mentions for N hours. |
| Min ADV (USD) | $500,000 | Minimum average daily volume for watchlist admission. |

#### Section 3: Signal Quality
| Parameter | Default | Effect |
|-----------|---------|--------|
| Signal quality threshold | 0.60 | Minimum composite score to fire a signal. Raise to reduce noise. |
| Sentiment strength min | 0.20 | Minimum |sentiment score| to contribute to aggregation. |
| Reactive price threshold | 0.02 | If price already moved >2% when signal fires, classify as reactive (penalised). |
| **Factor Weights** | | Must sum to 1.0 |
| Volume Z-score weight | 0.30 | Importance of mention spike vs baseline |
| Sentiment strength weight | 0.25 | Importance of sentiment polarity |
| Proactivity weight | 0.20 | Bonus for signals that fire *before* price moves |
| Price momentum weight | 0.15 | Importance of corroborating price movement |
| Cross-platform weight | 0.10 | Bonus when multiple platforms agree |

#### Section 4: Position Sizing
| Parameter | Default | Effect |
|-----------|---------|--------|
| Max position % of NLV | 2% | Maximum allocation per trade. **Halve this before going live.** |
| Half-Kelly fraction | 0.5 | Conservative sizing multiplier. 0.25 = quarter-Kelly. |
| Sigma target | 0.01 | Target daily vol contribution per position. |
| Max social allocation | 20% | Maximum portfolio % in social momentum trades total. |
| Max bid-ask spread (bps) | 100 | Rejects orders where spread > 1%. Tighten to 25bps for live. |
| Min ADV for orders (USD) | $1,000,000 | Minimum liquidity for execution. |

#### Section 5: Exit Rules
| Parameter | Default | Effect |
|-----------|---------|--------|
| Take profit % | 6% | Close position when unrealised P&L reaches +6%. |
| Trailing stop % | 2% | Close if price retraces 2% from high-water mark. |
| ATR multiplier | 2.0 | Stop loss = entry ± (2 × 14-day ATR). |
| Max hold hours | 48 | Hard time stop — close regardless of P&L after 48h. |
| Sentiment reversal threshold | -0.30 | Close LONG if sentiment drops below -0.30 (reversal). |
| Mention decay threshold | 0.20 | Close if current mentions < 20% of peak (trend fading). |

#### Section 6: Risk & Circuit Breakers
| Parameter | Default | Effect |
|-----------|---------|--------|
| Single trade loss limit | 1% | Emergency close if one position loses >1% of NLV. |
| Daily loss limit | 3% | Halt new trades if daily P&L < -3%. |
| Weekly loss limit | 7% | Reduce sizes 50% if weekly P&L < -7%. |
| Drawdown halt | 15% | Full halt if drawdown from HWM exceeds 15%. |
| VIX crisis threshold | 40 | Above this: 0% new position size (no trading). |
| VIX high fear | 30 | Above this: 25% of normal size. |
| VIX elevated | 25 | Above this: 50% of normal size. |
| VIX slightly elevated | 20 | Above this: 75% of normal size. |

**Save Configuration button:** Writes all changes to Redis. All running services reload on their next cycle.  
**Reset to Defaults button:** Reverts every parameter to the original `SystemConfig` defaults.

---

### 7.7 Page 6 — Parameter Optimization

Analyse historical performance to tune parameters. Has 4 tabs:

#### Tab 1: Run History
- Table of all saved config runs (each day a snapshot is saved automatically)
- Filter by mode (`paper` / `live` / `All`)
- Columns: date, Sharpe, win rate, total P&L, max drawdown, config hash
- Use this to compare which parameter sets performed best

#### Tab 2: Sensitivity Analysis
- Select any config parameter (x-axis) and any performance metric (y-axis)
- Scatter plot shows the relationship across all historical runs
- Pearson correlation coefficient tells you if the parameter matters
- **Interpretation:** Strong positive correlation between `signal_quality_threshold` and Sharpe → raise the threshold

#### Tab 3: Auto-Suggestions
- Analyses the N best-performing sessions
- Computes the median parameter values from those sessions
- Compares to your current config
- Shows a table of suggested changes with expected impact
- **How to use:** Click **Apply** on suggestions you agree with — they update the live config

#### Tab 4: Grid Search
- Define ranges for up to 3 parameters (quality threshold, take profit %, ATR multiplier)
- Set how many days of trade history to evaluate against
- Click **Run Grid Search** — tests all combinations against historical trade outcomes
- Results table sorted by Sharpe ratio
- **Apply best settings to config** button → updates live config with the winning combination

> ⚠️ Grid search is a simple in-sample backtest on your recent paper trades, not a
> full walk-forward simulation. Use it as a guide, not a guarantee. Always re-test
> any new parameters for at least 2 days in paper mode before going live.

---

## Part 8 — Transitioning from Paper to Live Trading

### 8.1 Prerequisites Checklist

Complete **all** items before switching to live:

- [ ] 5-day paper run completed with Sharpe ≥ 0.5
- [ ] Win rate ≥ 45% over at least 20 trades
- [ ] No circuit breaker triggers during the paper run (or all triggers were legitimate)
- [ ] Maximum drawdown during paper run < 5%
- [ ] You have reviewed every exit reason — no unexpected EMERGENCY exits
- [ ] IBKR live account funded (recommend $25,000 minimum for PDT rule compliance in US)
- [ ] You have read and understood IBKR's margin requirements for your account type
- [ ] You have set up 2FA on your IBKR account
- [ ] You have a written emergency procedure (who to call, how to close all positions if internet goes down)

### 8.2 Risk Parameter Tightening for Live

Before switching, update these in Streamlit → Config (Page 5):

| Parameter | Paper default | Live recommendation | Why |
|-----------|--------------|---------------------|-----|
| Max position % of NLV | 2% | **0.5%** | Real money — start tiny |
| Half-Kelly fraction | 0.5 | **0.25** | Quarter-Kelly for first month |
| Daily loss limit | 3% | **1%** | Tighter stops on real capital |
| Single trade loss limit | 1% | **0.5%** | |
| Weekly loss limit | 7% | **3%** | |
| Drawdown halt | 15% | **8%** | |
| Max bid-ask spread (bps) | 100 | **25** | Real fills cost more |
| Max hold hours | 48 | **24** | Shorter holds reduce overnight risk |
| Signal quality threshold | 0.60 | **0.70** | Higher bar for real money |

Click **Save Configuration** after making changes.

### 8.3 Switching the Execution Service to Live

**Step 1 — Update `.env`:**

```dotenv
IBKR_PORT=4001          # IB Gateway live port (NOT 4002)
IBKR_PAPER=false
TRADING_MODE=live
```

**Step 2 — Switch IB Gateway to live login:**
1. Quit IB Gateway
2. Relaunch and log in with your **live** account credentials (NOT paper)
3. Confirm in Gateway settings: API socket port = **4001**
4. Verify the account shown is your live account number

**Step 3 — Stop paper execution, start live:**

```bash
# If execution was running in Docker:
docker compose stop execution

# If running directly on host, Ctrl+C the existing process

# Start with --ibkr flag (now connects to live account via port 4001)
source .venv/bin/activate
python -m social_trading.services.execution_service --ibkr
```

Check the log output — you should see:
```
Connected to IBKR port=4001 clientId=10
```
and your live account balance in the account state.

**Step 4 — Verify account state in Streamlit:**

Open the main dashboard. The equity displayed should match your IBKR live account balance.

**Step 5 — Seed with conservative watchlist:**

For the first live week, trade only the most liquid tickers:

```bash
python scripts/seed_watchlist.py --tickers AAPL MSFT SPY QQQ
```

Expand the watchlist only after the first week proves stable.

### 8.4 First Week of Live Trading — Monitoring Protocol

**Every morning before market open (9:00–9:30 AM ET):**
1. Open Streamlit main dashboard — verify equity is correct
2. Check circuit breaker state is `NORMAL`
3. Review overnight logs: `docker logs social_trading-execution-1 --tail 100`
4. Confirm IB Gateway is still connected (check Gateway status bar)

**During market hours (9:30 AM – 4:00 PM ET):**
1. Check Streamlit every 30 minutes for the first week
2. Watch the Positions page — no single position should exceed 2% of equity
3. If you see an unexpected position, use the **Close [TICKER]** button immediately
4. Keep the terminal with the execution service visible

**Market close routine (4:00–4:30 PM ET):**
1. Record today's P&L in your trading journal
2. Streamlit → Trades page — review all closed trades
3. Run the Sharpe snapshot (Part 6 query) weekly
4. Use Streamlit → Optimize → Auto-Suggestions to check if parameters need tuning

**Emergency procedure:**
- Internet outage: log in to IBKR web portal directly (https://www.interactivebrokers.com) and close positions manually
- Server crash: `ssh user@server`, run `docker compose restart execution`, or close positions via IBKR portal
- Runaway losses: Call IBKR support (24/7): +1 877-442-2757 to have them freeze the account

### 8.5 Scaling Up

Do **not** increase position sizes until you have:
- 4 consecutive profitable weeks
- Sharpe ≥ 0.8 over 20+ trades
- Maximum single-day loss < 0.5% of total equity

When ready to scale:
1. Increase `max_position_pct` by 0.25% increments (e.g., 0.5% → 0.75%)
2. Wait 1 week at each level before increasing further
3. Never exceed 2% per position in the first 6 months

---

## Part 9 — Other Considerations

### 9.1 Network / IP Considerations

**Residential IP:**
- X API: residential IPs are fine for Basic tier
- Reddit: residential IPs are fine; Reddit bans IPs that make unusual volumes of requests
- StockTwits: residential IPs are fine
- IBKR: connects to `localhost` — no external IP needed

**VPS / Cloud Server:**
- Use a server in **US-East (New York/Virginia)** for lowest latency to US market data
- Open inbound ports: **8501** (Streamlit), **3000** (Grafana), **9090** (Prometheus)
- Keep port **7497/4002** closed to the internet (IBKR API is localhost-only)
- Firewall: allow SSH (22), 8501, 3000, 9090; block everything else

**If running on a remote server, use SSH tunnels for Streamlit and Grafana:**

```bash
# On your local machine:
ssh -L 8501:localhost:8501 -L 3000:localhost:3000 user@your-server-ip
```

Then open `http://localhost:8501` and `http://localhost:3000` locally.

### 9.2 Market Hours Awareness

- The system runs 24/7 but IBKR paper account only executes during market hours:
  **9:30 AM – 4:00 PM ET, Monday–Friday**
- Signals generated outside market hours are queued in `selected_signals` Redis stream
  and will be consumed when the execution loop runs — but IBKR will reject them
  if the market is closed. This is expected behavior.
- Pre-market (4–9:30 AM ET) and after-hours (4–8 PM ET): monitoring continues
  but no new positions open

### 9.3 API Rate Limit Budget

| Platform | Limit | Our usage | Buffer |
|----------|-------|-----------|--------|
| X Basic | 10,000 tweets/month | ~3,000/month (5 tickers × 20/day × 30) | 3× |
| Reddit | 100 req/min | ~20 req/min | 5× |
| StockTwits | 200 req/hour | ~10 req/hour | 20× |
| yfinance | Unofficial (no hard limit) | ~100 req/hour | comfortable |

Monitor your X API usage at https://developer.twitter.com/en/portal/usage

### 9.4 Logs and Debugging

```bash
# Follow all service logs simultaneously
docker compose logs -f --tail=50

# One specific service
docker compose logs -f execution --tail=100

# Save logs to file for review
docker compose logs > run_log_$(date +%Y%m%d).txt
```

### 9.5 Emergency Stop

If anything goes wrong, use the Streamlit UI **HALT** button (Sidebar → 🛑 Halt New Positions).
For a full emergency stop:

```bash
# Close all positions immediately via Redis
redis-cli publish trading:commands '{"cmd":"CLOSE_ALL","payload":{},"ts":"now"}'

# Or stop all services
docker compose stop execution
```

### 9.6 Persistence and Crash Recovery

All data is persisted:
- PostgreSQL: `postgres_data` Docker volume — trades, signals, raw posts
- Redis: `redis_data` volume with `appendonly yes` — stream data, watchlist, config
- On restart, all services recover automatically (`restart: unless-stopped`)

The execution service reconnects to IBKR on startup. If IBKR Gateway restarts,
restart the execution service:
```bash
docker compose restart execution
# or if running directly:
# Ctrl+C, then re-run: python -m social_trading.services.execution_service --ibkr
```


---

## Part 1 — Social Media API Setup

### 1.1 X (Twitter) API

You need **X API v2 Basic or Pro tier** ($100/month Basic, $5,000/month Pro).
The free tier does NOT provide streaming or search endpoints needed here.

**Steps:**

1. Go to https://developer.twitter.com/en/portal/dashboard
2. Click **+ Create Project** → name it `social-trading`
3. Create an App inside the project → choose **Production** environment
4. Under **Keys and Tokens**, generate:
   - `Bearer Token` → maps to `X_BEARER_TOKEN`
   - `API Key & Secret` → maps to `X_API_KEY` / `X_API_SECRET`
   - `Access Token & Secret` → maps to `X_ACCESS_TOKEN` / `X_ACCESS_SECRET`
5. Under **App Settings → Authentication settings**, enable **Read** permissions
6. Under **Product Settings**, confirm `Basic` tier (or higher) is active

**Permissions needed:**  `Read` on public tweets is sufficient.

**Rate limits at Basic tier:**
- Search (recent): 10,000 tweets/month, 1 request/second
- The ingest service respects `x-rate-limit-reset` headers automatically

> ⚠️ If you only want to test the pipeline without X costs, set
> `X_BEARER_TOKEN=` empty — the ingest service will skip Twitter and use
> Reddit + StockTwits only.

---

### 1.2 Reddit API

Reddit API is **free** with reasonable rate limits.

**Steps:**

1. Log in at https://www.reddit.com/prefs/apps
2. Click **create another app** (bottom of page)
3. Fill in:
   - **name:** `social-trading`
   - **type:** select `script`
   - **redirect uri:** `http://localhost:8080`
   - **description:** anything
4. Click **create app**
5. Copy values:
   - The string under the app name (14-char code) → `REDDIT_CLIENT_ID`
   - The `secret` field → `REDDIT_CLIENT_SECRET`
6. Set `REDDIT_USER_AGENT` to a descriptive string, e.g.:
   `social-trading-bot/0.1 by u/YourUsername`

**Rate limits:** 100 requests/minute on OAuth (well within ingest cadence)

**Default subreddits monitored:** `wallstreetbets+stocks+options+investing`  
You can override by modifying `DEFAULT_SUBREDDITS` in `src/social_trading/ingest/sources/reddit.py`.

---

### 1.3 StockTwits API

StockTwits offers a **free public API** for reading the stream.

**Steps:**

1. Register at https://api.stocktwits.com/developers/apps/new
2. Create a new application
3. Copy the **OAuth Token** → `STOCKTWITS_TOKEN`

> StockTwits token is optional — the ingest service gracefully skips it if
> the env var is empty.

---

## Part 2 — Interactive Brokers Setup (Paper Trading)

### 2.1 Create a Paper Trading Account

1. Log in to https://www.interactivebrokers.com
2. Navigate to **Account Management → Paper Trading Account** (or create one at signup)
3. Fund the paper account with a simulated amount — recommend **$100,000**

### 2.2 Install TWS or IB Gateway

Two options; **IB Gateway is preferred** (headless, lower resource usage):

**Option A — IB Gateway (recommended for servers):**
1. Download from: https://www.interactivebrokers.com/en/trading/ibgateway-latest.php
2. Install and launch
3. Log in with your **paper trading** credentials (separate login from live)
4. Navigate to **Configure → Settings → API**:
   - ☑ Enable ActiveX and Socket Clients
   - ☑ Allow connections from localhost only
   - Socket port: **4002** (paper) or **4001** (live)
   - ☑ Read-Only API: **OFF** (must be off to place orders)
   - Master Client ID: leave as **0**

**Option B — Trader Workstation (TWS):**
1. Download from: https://www.interactivebrokers.com/en/trading/tws.php
2. Install and launch with **paper trading** login
3. Navigate to **Edit → Global Configuration → API → Settings**:
   - ☑ Enable ActiveX and Socket Clients
   - Socket port: **7497** (paper TWS default)
   - ☑ Allow connections from localhost only

### 2.3 Configure Port in .env

```
IBKR_PORT=4002          # IB Gateway paper
# OR
IBKR_PORT=7497          # TWS paper
```

> ⚠️ The execution service uses `network_mode: host` in docker-compose so it
> can reach `127.0.0.1:7497/4002`. Do NOT change this.

---

## Part 3 — Environment Setup

### 3.1 Machine Prerequisites

```bash
# macOS
brew install docker docker-compose git python@3.14

# Ubuntu 22.04
sudo apt update && sudo apt install -y docker.io docker-compose-v2 git python3.14 python3.14-venv
sudo usermod -aG docker $USER   # then re-login
```

### 3.2 Clone and Install

```bash
git clone https://github.com/nihanli/social_trading.git
cd social_trading
python3.14 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### 3.3 Create .env File

Copy the example and fill in your values:

```bash
cp .env.example .env
nano .env          # or use your editor
```

Complete `.env` for paper trading run:

```dotenv
# ── Database ────────────────────────────────────────────────────────────────
DB_HOST=localhost
DB_PORT=5432
DB_NAME=trading
DB_USER=trader
DB_PASSWORD=changeme          # change this in production

# ── Redis ───────────────────────────────────────────────────────────────────
REDIS_URL=redis://localhost:6379/0

# ── X (Twitter) API ─────────────────────────────────────────────────────────
X_BEARER_TOKEN=AAAAAAAAAAAAAAAAAAAAAxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
X_API_KEY=your_api_key_here
X_API_SECRET=your_api_secret_here
X_ACCESS_TOKEN=your_access_token_here
X_ACCESS_SECRET=your_access_secret_here

# ── Reddit API ───────────────────────────────────────────────────────────────
REDDIT_CLIENT_ID=abc123xyz456
REDDIT_CLIENT_SECRET=your_secret_here
REDDIT_USER_AGENT=social-trading-bot/0.1 by u/YourUsername

# ── StockTwits ────────────────────────────────────────────────────────────────
STOCKTWITS_TOKEN=your_token_here

# ── Interactive Brokers ──────────────────────────────────────────────────────
IBKR_HOST=127.0.0.1
IBKR_PORT=7497              # 7497=TWS paper, 4002=Gateway paper
IBKR_CLIENT_ID=10
IBKR_PAPER=true

# ── System ───────────────────────────────────────────────────────────────────
LOG_LEVEL=INFO
TRADING_MODE=paper
PAPER_INITIAL_CASH=100000

# ── Grafana ───────────────────────────────────────────────────────────────────
GF_SECURITY_ADMIN_PASSWORD=your_secure_grafana_password
```

> ⚠️ Never commit `.env` to git. It is already in `.gitignore`.

---

## Part 4 — Command Sequence (Launch Order)

Run these in order. Each step depends on the previous being healthy.

### Step 1 — Start Infrastructure

```bash
# Start postgres and redis
make up
# OR equivalently:
docker compose up -d postgres redis

# Verify both are healthy (wait ~10 seconds)
docker compose ps
```

Expected output: both show `(healthy)`.

### Step 2 — Run Database Migrations

```bash
make migrate
# OR:
python migrations/migrate.py
```

Expected: `Migrations applied: 001_initial_schema.sql, 002_config_runs.sql`

### Step 3 — Seed the Watchlist

```bash
source .venv/bin/activate
python scripts/seed_watchlist.py

# Optional: override default tickers
python scripts/seed_watchlist.py --tickers AAPL TSLA NVDA AMD MSFT SPY QQQ
```

Verify: `redis-cli smembers watchlist:active` should list your tickers.

### Step 4 — Start IB Gateway / TWS

Launch IB Gateway (or TWS) manually on the host machine and log in with
your paper trading credentials. Confirm the API socket is listening:

```bash
# Verify TWS/Gateway is reachable
nc -z 127.0.0.1 7497 && echo "TWS reachable" || echo "TWS NOT reachable"
# OR for Gateway:
nc -z 127.0.0.1 4002 && echo "Gateway reachable" || echo "Gateway NOT reachable"
```

### Step 5 — Start Application Services

```bash
# Start all 9 services (ingest, nlp, signal, risk, execution, streamlit, prometheus, grafana)
make services-up
# OR:
docker compose up -d

# For the first run, rebuild images first:
docker compose build --no-cache
docker compose up -d
```

Wait ~30 seconds, then verify all containers are running:

```bash
docker compose ps
```

All 9 services should show `Up` or `(healthy)`.

### Step 6 — Start Execution in IBKR Mode

The execution service starts in paper mode by default. To connect to IBKR:

```bash
# Stop the default execution container
docker compose stop execution

# Run execution service directly on host (required for IBKR localhost access)
source .venv/bin/activate
python -m social_trading.services.execution_service --ibkr
```

Leave this terminal open — this is the live process.

> Why not Docker for execution? Docker networking isolates containers from
> `localhost:7497`. Running directly on host bypasses this.
> Alternatively, keep `network_mode: host` in docker-compose (already configured)
> and it will work on Linux. On macOS, run directly.

### Step 7 — Open Monitoring Dashboards

| Dashboard | URL | Credentials |
|-----------|-----|-------------|
| Streamlit UI | http://localhost:8501 | none |
| Grafana | http://localhost:3000 | admin / (your GF_SECURITY_ADMIN_PASSWORD) |
| Prometheus | http://localhost:9090 | none |

In Grafana, navigate to **Dashboards → Social Trading → Portfolio Overview**.

---

## Part 5 — Verifying the System is Working

### 5.1 Check Data is Flowing (within 5 minutes of startup)

```bash
# Posts being ingested?
docker logs social_trading-ingest-1 --tail 20

# Redis streams have data?
redis-cli xlen raw_social
redis-cli xlen sentiment_signals
redis-cli xlen strategy_signals
redis-cli xlen selected_signals

# Active tickers in watchlist?
redis-cli smembers watchlist:active
```

Expected: `raw_social` should have entries within 2–5 minutes.

### 5.2 Check Sentiment is Being Scored

```bash
docker logs social_trading-nlp-1 --tail 20
redis-cli xlen sentiment_signals
```

### 5.3 Check Signals are Being Generated

```bash
docker logs social_trading-signal-1 --tail 20
redis-cli xlen strategy_signals
```

### 5.4 Check Risk Service is Approving Signals

```bash
docker logs social_trading-risk-1 --tail 20
redis-cli xlen selected_signals
```

### 5.5 Check a Trade was Placed

```bash
# Paper positions (last 10 trades)
redis-cli lrange trades:recent 0 9

# Account state
redis-cli hgetall account:state
```

---

## Part 6 — 5-Day Paper Trading Run Checklist

### Day 1 (Monday) — Baseline

- [ ] All services green in `docker compose ps`
- [ ] At least 50 posts per hour in `raw_social` stream
- [ ] At least one signal generated by end of day
- [ ] IBKR paper account shows correct equity ($100,000)
- [ ] Grafana Equity Curve panel shows a flat line (no trades yet may be OK)
- [ ] Check circuit breaker state: `redis-cli get circuit:state` → `{"state": "NORMAL"}`

### Day 2–4 — Active Monitoring (twice daily: 9:30 AM and 3:30 PM ET)

Each session, verify:
- [ ] `redis-cli hgetall account:state` — equity moving, no runaway losses
- [ ] Streamlit → Positions page — max 5–10 open positions
- [ ] Streamlit → Signals page — signal quality distribution looks reasonable (not all 0 or 1)
- [ ] Grafana → System Health — no RED alerts firing
- [ ] Log check: `docker logs social_trading-execution-1 --tail 50`

### Day 5 (Friday) — Evaluation Gate

Run the Sharpe > 0.5 gate calculation:

```bash
source .venv/bin/activate
python - <<'EOF'
import psycopg2, os
from dotenv import load_dotenv
load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST", "localhost"),
    dbname=os.getenv("DB_NAME", "trading"),
    user=os.getenv("DB_USER", "trader"),
    password=os.getenv("DB_PASSWORD", "changeme"),
)
cur = conn.cursor()
cur.execute("""
    SELECT
        COUNT(*)                          AS trades,
        AVG(pnl_pct)                      AS mean_return,
        STDDEV(pnl_pct)                   AS std_return,
        AVG(pnl_pct) / NULLIF(STDDEV(pnl_pct), 0) * SQRT(252) AS annualised_sharpe,
        SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END)::float / COUNT(*) AS win_rate
    FROM trades
    WHERE closed_at >= NOW() - INTERVAL '5 days'
""")
row = cur.fetchone()
print(f"Trades:    {row[0]}")
print(f"Mean ret:  {row[1]:.4%}")
print(f"Std ret:   {row[2]:.4%}")
print(f"Sharpe:    {row[3]:.2f}")
print(f"Win rate:  {row[4]:.1%}")
conn.close()
EOF
```

**Gate: Sharpe ≥ 0.5 before proceeding to live.**

---

## Part 7 — Other Considerations

### 7.1 Network / IP Considerations

**Residential IP:**
- X API: residential IPs are fine for Basic tier
- Reddit: residential IPs are fine; Reddit bans IPs that make unusual volumes of requests
- StockTwits: residential IPs are fine
- IBKR: connects to `localhost` — no external IP needed

**VPS / Cloud Server:**
- Use a server in **US-East (New York/Virginia)** for lowest latency to US market data
- Open inbound ports: **8501** (Streamlit), **3000** (Grafana), **9090** (Prometheus)
- Keep port **7497/4002** closed to the internet (IBKR API is localhost-only)
- Firewall: allow SSH (22), 8501, 3000, 9090; block everything else

**If running on a remote server, use SSH tunnels for Streamlit and Grafana:**

```bash
# On your local machine:
ssh -L 8501:localhost:8501 -L 3000:localhost:3000 user@your-server-ip
```

Then open `http://localhost:8501` and `http://localhost:3000` locally.

### 7.2 Market Hours Awareness

- The system runs 24/7 but IBKR paper account only executes during market hours:
  **9:30 AM – 4:00 PM ET, Monday–Friday**
- Signals generated outside market hours are queued in `selected_signals` Redis stream
  and will be consumed when the execution loop runs — but IBKR will reject them
  if the market is closed. This is expected behavior.
- Pre-market (4–9:30 AM ET) and after-hours (4–8 PM ET): monitoring continues
  but no new positions open

### 7.3 API Rate Limit Budget

| Platform | Limit | Our usage | Buffer |
|----------|-------|-----------|--------|
| X Basic | 10,000 tweets/month | ~3,000/month (5 tickers × 20/day × 30) | 3× |
| Reddit | 100 req/min | ~20 req/min | 5× |
| StockTwits | 200 req/hour | ~10 req/hour | 20× |
| yfinance | Unofficial (no hard limit) | ~100 req/hour | comfortable |

Monitor your X API usage at https://developer.twitter.com/en/portal/usage

### 7.4 Logs and Debugging

```bash
# Follow all service logs simultaneously
docker compose logs -f --tail=50

# One specific service
docker compose logs -f execution --tail=100

# Save logs to file for review
docker compose logs > run_log_$(date +%Y%m%d).txt
```

### 7.5 Emergency Stop

If anything goes wrong, use the Streamlit UI **HALT** button (Sidebar → 🛑 Halt New Positions).
For a full emergency stop:

```bash
# Close all positions immediately via Redis
redis-cli publish trading:commands '{"cmd":"CLOSE_ALL","payload":{},"ts":"now"}'

# Or stop all services
docker compose stop execution
```

### 7.6 Persistence and Crash Recovery

All data is persisted:
- PostgreSQL: `postgres_data` Docker volume — trades, signals, raw posts
- Redis: `redis_data` volume with `appendonly yes` — stream data, watchlist, config
- On restart, all services recover automatically (`restart: unless-stopped`)

The execution service reconnects to IBKR on startup. If IBKR Gateway restarts,
restart the execution service:
```bash
docker compose restart execution
# or if running directly:
# Ctrl+C, then re-run: python -m social_trading.services.execution_service --ibkr
```

### 7.7 Before Going Live (after 5-day paper run passes gate)

1. Change `.env`:
   ```
   IBKR_PAPER=false
   IBKR_PORT=4001        # IB Gateway live port
   TRADING_MODE=live
   ```
2. Log in to IB Gateway with your **live** account credentials
3. Reduce position sizes: in Streamlit → Config, set `max_position_pct=0.005` (0.5%)
4. Set tight circuit breakers: `loss_limit_daily=0.01` (1%)
5. Start with only 2–3 highly liquid tickers (AAPL, MSFT, SPY)
6. Monitor every 30 minutes for the first week
