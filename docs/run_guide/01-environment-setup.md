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

#### StockTwits — Free, no account required ✅

`StockTwitsDataSource` uses StockTwits' **public unauthenticated endpoints** for
both trending discovery and spike detection. No sign-up or API key is required.
It is **enabled by default** and serves as the primary replacement for the X Counts API:

- `/streams/symbol/{TICKER}.json` — counts new messages per cycle (Z-score spike detection)
- `/streams/trending.json` — trending ticker discovery

Nothing to configure. Rate limit: ~200 requests/hour (unauthenticated). The default
5-minute poll interval for 50 tickers = ~10 req/5 min, well within quota.

#### Bluesky — Free, requires free bsky.app account

`BlueskyDataSource` uses the official AT Protocol API (open, free, no usage fees).
It supplements StockTwits with a second independent spike-detection signal.

1. Create a free account at https://bsky.app
2. Go to **Settings → Privacy and Security → App Passwords**
3. Click **Add App Password** → name it `social-trading` → copy the password
4. Set in `.env`:
   ```dotenv
   BLUESKY_HANDLE=yourhandle.bsky.social
   BLUESKY_APP_PASSWORD=xxxx-xxxx-xxxx-xxxx
   ```

The `atproto` SDK is included in the project dependencies. No additional installation needed.

#### Reddit API — Free (optional)

1. Go to https://www.reddit.com/prefs/apps
2. Click **create another app** → type: `script`, redirect: `http://localhost:8080`
3. Copy the 14-char code under the app name → `REDDIT_CLIENT_ID`
4. Copy the `secret` field → `REDDIT_CLIENT_SECRET`
5. Set `REDDIT_USER_AGENT` to e.g. `social-trading-bot/0.1 by u/YourUsername`

Default subreddits: `wallstreetbets+stocks+options+investing`

#### X (Twitter) API — ⚠️ Disabled by default (pay-per-use billing)

X migrated to **pay-per-use pricing with no free tier** in 2025. The Counts endpoint
used for spike detection now costs **$0.005 per request**:

- 50 tickers × 288 polls/day × $0.005 = **~$72/day (~$2,160/month)** for Counts alone

**X is disabled by default** (`x_api_enabled = False` in SystemConfig) even when
`X_BEARER_TOKEN` is set, to prevent accidental billing. To enable:

1. Ensure you have a paid X API plan at https://developer.twitter.com
2. Set `X_BEARER_TOKEN` in `.env`
3. In the **Config UI → X API section**, toggle **Enable X API** (or set
   `x_api_enabled = True` in Redis directly)

StockTwits + Bluesky provide equivalent spike detection at zero cost and are the
recommended replacement.

#### StockTwits (legacy token) — ⚠️ No longer available for new registrations

StockTwits closed new developer API account creation. The `STOCKTWITS_TOKEN` env
var is no longer used — the source now operates via public unauthenticated endpoints
and does not require a token. Remove it from `.env` if present.

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

# ── X (Twitter) — disabled by default; enable via x_api_enabled in Config UI ──
X_BEARER_TOKEN=                    # set but x_api_enabled must be True to activate
X_API_KEY=
X_API_SECRET=
X_ACCESS_TOKEN=
X_ACCESS_SECRET=

# ── Reddit ────────────────────────────────────────────────────────────────────
REDDIT_CLIENT_ID=                  # leave empty to disable Reddit source
REDDIT_CLIENT_SECRET=
REDDIT_USER_AGENT=social-trading-bot/0.1 by u/YourUsername

# ── StockTwits ────────────────────────────────────────────────────────────────
# No token required — public unauthenticated endpoints are used automatically.
# STOCKTWITS_TOKEN is no longer used; remove it if set.

# ── Bluesky ───────────────────────────────────────────────────────────────────
BLUESKY_HANDLE=                    # e.g. yourhandle.bsky.social
BLUESKY_APP_PASSWORD=              # from bsky.app Settings → App Passwords

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
