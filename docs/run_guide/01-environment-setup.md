## Part 1 — Environment Setup (One-Time)

Do this once on a new machine. Skip to Part 2 if already set up.

### 1.1 Install System Dependencies

**macOS:**
```bash
# Install Homebrew first if not installed: https://brew.sh
brew install git python@3.14

# Install Docker Desktop (required — brew install docker is NOT enough)
# Download from: https://www.docker.com/products/docker-desktop/
# Launch Docker Desktop and keep it running in the menu bar
```

**Ubuntu 22.04:**
```bash
sudo apt update && sudo apt install -y docker.io docker-compose-v2 git python3.14 python3.14-venv
sudo usermod -aG docker $USER   # then log out and back in
```

### 1.2 Clone and Install Python Environment

```bash
git clone https://github.com/nihanli/social_trading.git
cd social_trading

# Create isolated Python environment
python3.14 -m venv .venv
source .venv/bin/activate          # run this every new terminal session

# Install the project and all dependencies (run once)
pip install -e ".[dev]"
```

### 1.3 Social Media API Keys

You only need the APIs you intend to use. The system runs with any combination —
a missing key simply disables that source (logged as a warning, no crash).

#### X (Twitter) API — $100/month Basic tier required

1. Go to https://developer.twitter.com/en/portal/dashboard
2. Create a project → create an app inside it → choose **Production** environment
3. Under **Keys and Tokens**, generate:
   - `Bearer Token` → `X_BEARER_TOKEN`
   - `API Key & Secret` → `X_API_KEY` / `X_API_SECRET`
   - `Access Token & Secret` → `X_ACCESS_TOKEN` / `X_ACCESS_SECRET`
4. Confirm `Basic` tier (or higher) is active

Rate limits at Basic tier: 10,000 tweets/month, 1 req/second (our usage: ~3,000/month).

#### Reddit API — Free

1. Go to https://www.reddit.com/prefs/apps
2. Click **create another app** → type: `script`, redirect: `http://localhost:8080`
3. Copy the 14-char code under the app name → `REDDIT_CLIENT_ID`
4. Copy the `secret` field → `REDDIT_CLIENT_SECRET`
5. Set `REDDIT_USER_AGENT` to e.g. `social-trading-bot/0.1 by u/YourUsername`

Default subreddits: `wallstreetbets+stocks+options+investing`

#### StockTwits API — ⚠️ No longer available for new registrations

StockTwits closed new developer API account creation. Existing tokens continue
to work; if you have one, set `STOCKTWITS_TOKEN` in `.env`.  
For new deployments, the three sources below replace StockTwits as the primary
trending-ticker discovery mechanism.

#### Yahoo Finance Screener — Free, no key required

No sign-up needed. `YFinanceScreenerDataSource` uses the `yfinance` library
(already installed) to query Yahoo's `most_actives`, `day_gainers`, and
`day_losers` screeners each cycle. Nothing to configure beyond optionally
tuning `yfinance_screener_count` in the UI Config page (default: 50).

#### Alpha Vantage — Free API key

Provides the `TOP_GAINERS_LOSERS` endpoint (top gainers, losers, and most
actively traded — 20 tickers each per call). The free tier allows **25
requests/day**; the service caches results in Redis for 1 hour by default so
normal usage stays well within quota.

1. Register at https://www.alphavantage.co/support/#api-key (instant, no credit
   card)
2. Copy the API key → `ALPHA_VANTAGE_API_KEY` in `.env`

Cache TTL and all other parameters are tunable from the UI **Config → 2b**
page without restarting.

#### IBKR Market Scanner — Real-time, requires IBKR account

`IBKRScannerDataSource` runs `HOT_BY_VOLUME` and `TOP_PERC_GAIN` scanner
subscriptions against TWS/IB Gateway, returning up to 50 real-time results per
scan. The same IB Gateway instance used for order execution also serves the
scanner — no second installation needed.

Setup is covered in §1.4 below. The scanner uses a **separate `clientId`**
(default: `99`) from the execution layer to avoid connection conflicts. Override
with `IBKR_SCANNER_CLIENT_ID` in `.env` if needed.

### 1.4 Interactive Brokers Setup

**Create a paper trading account:**
1. Log in to https://www.interactivebrokers.com
2. Navigate to **Account Management → Paper Trading Account**
3. Set simulated balance to **$100,000**

**Install IB Gateway (recommended — lightweight, headless):**
1. Download from: https://www.interactivebrokers.com/en/trading/ibgateway-latest.php
2. Launch and log in with your **paper** credentials
3. Go to **Configure → Settings → API**:
   - ☑ Enable ActiveX and Socket Clients
   - Socket port: **4002** (paper Gateway) or **7497** (paper TWS)
   - ☑ Allow connections from localhost only
   - Read-Only API: **OFF** (must be off to place orders)

> **Scanner note:** Both the execution layer and the IBKR Market Scanner share
> the same TWS/Gateway connection but use different `clientId` values. The
> execution layer defaults to `clientId=10` (`IBKR_CLIENT_ID`); the scanner
> defaults to `clientId=99` (`IBKR_SCANNER_CLIENT_ID`). Keep them distinct to
> avoid `"Already connected"` errors.

### 1.5 Create the .env File

```bash
cp .env.example .env
nano .env    # fill in your values
```

Complete `.env` reference:

```dotenv
# ── Database ─────────────────────────────────────────────────────────────────
DB_HOST=localhost
DB_PORT=5432
DB_NAME=trading
DB_USER=trader
DB_PASSWORD=changeme

# ── Redis ─────────────────────────────────────────────────────────────────────
REDIS_URL=redis://localhost:6379/0

# ── X (Twitter) ───────────────────────────────────────────────────────────────
X_BEARER_TOKEN=                    # leave empty to disable X source
X_API_KEY=
X_API_SECRET=
X_ACCESS_TOKEN=
X_ACCESS_SECRET=

# ── Reddit ────────────────────────────────────────────────────────────────────
REDDIT_CLIENT_ID=                  # leave empty to disable Reddit source
REDDIT_CLIENT_SECRET=
REDDIT_USER_AGENT=social-trading-bot/0.1 by u/YourUsername

# ── StockTwits ────────────────────────────────────────────────────────────────
STOCKTWITS_TOKEN=                  # leave empty to disable StockTwits source

# ── Trending Ticker Sources ───────────────────────────────────────────────────
# Yahoo Finance screener: no key needed — zero-config, enabled by default

# Alpha Vantage free key — get one at alphavantage.co/support/#api-key
# Leave empty to disable (Yahoo Finance + IBKR scanner still run)
ALPHA_VANTAGE_API_KEY=

# ── Interactive Brokers ───────────────────────────────────────────────────────
IBKR_HOST=127.0.0.1        # shared by execution layer and market scanner

# Execution layer
IBKR_PORT=7497          # 7497=TWS paper, 4002=Gateway paper, 4001=Gateway live
IBKR_CLIENT_ID=10
IBKR_PAPER=true

# Market Scanner (discovery) — separate client ID avoids "Already connected" errors
IBKR_SCANNER_PORT=7497             # 7497=TWS paper, 7496=TWS live, 4002=Gateway paper, 4001=Gateway live
IBKR_SCANNER_CLIENT_ID=99

# ── System ────────────────────────────────────────────────────────────────────
LOG_LEVEL=INFO
TRADING_MODE=paper
PAPER_INITIAL_CASH=100000
GF_SECURITY_ADMIN_PASSWORD=changeme_grafana
```

> ⚠️ Never commit `.env` to git — it is already in `.gitignore`.

### 1.6 First-Time Database Initialisation

Run once after setting up `.env`:

```bash
source .venv/bin/activate
make up                          # start Postgres + Redis
make migrate                     # create database tables
python scripts/seed_watchlist.py # seed default ticker watchlist
```

Verify the watchlist was seeded:
```bash
redis-cli smembers watchlist:active
```


---

[Index](README.md) &nbsp;|&nbsp; [02-run-workflows.md](02-run-workflows.md) →
