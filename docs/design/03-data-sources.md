## 3. Social Media Data Sources & APIs

### Architecture: Dynamic Watchlist + Two-Tier Collection

> ⚠️ **Cost trap to avoid:** Connecting X's Filtered Stream continuously charges **$0.005 per
> post delivered**. For 20 tickers during market hours this quickly reaches $20–50/day on
> volatile sessions. The correct approach is a two-tier design where cheap volume polls trigger
> expensive content pulls only when a genuine spike is detected.

The system does **not** require the trader to supply a static ticker list. A discovery layer
continuously surfaces candidates from Reddit and StockTwits (both free), passes them through
a liquidity gate, and maintains a self-updating active watchlist in Redis. The trader may
optionally pin tickers they always want monitored.

```
┌───────────────────────────────────────────────────────────────────┐
│  DISCOVERY LAYER  (always running, always free)                   │
│                                                                   │
│  Reddit stream regex ──┐                                          │
│  StockTwits trending ──┼──► Candidate pool (Redis ZSET)          │
│  Trader seed list   ───┘         │                                │
│                                  │ passes liquidity gate?         │
│                                  │ (ADV>$500K, mcap>$50M,         │
│                                  │  spread<1%)                    │
│                                  ▼                                │
│              ACTIVE WATCHLIST  (Redis ZSET, score = last_seen)   │
│              auto-expires tickers silent for 48h                  │
│                        │                                          │
│       ┌────────────────┴──────────────────┐                       │
│       ▼                                   ▼                       │
│  TIER 1 — Volume Polling           TIER 2 — Content Pull          │
│  X Counts (free, per watchlist)    X Search ($0.005/post)         │
│  StockTwits symbol polls (free)    StockTwits symbol stream       │
│       │                                   │                       │
│       └──── Z-score > 2.0 ───────────────►│                       │
│                                           ▼                       │
│                              NLP pipeline → signal engine         │
└───────────────────────────────────────────────────────────────────┘
```

### 3a. Watchlist Manager

The `WatchlistManager` is the central registry all data sources read and write.
It lives in Redis so every microservice shares a single consistent view.

```python
import time, json, logging
import redis
import yfinance as yf   # free — used only for liquidity gate, not trading data

log = logging.getLogger(__name__)

class WatchlistManager:
    WATCHLIST_KEY  = "watchlist:active"     # ZSET  score = last_seen epoch
    CANDIDATE_KEY  = "watchlist:candidates" # ZSET  score = first_seen epoch
    SEED_KEY       = "watchlist:seed"       # SET   permanent pins from trader

    # Liquidity gate thresholds — loaded from SystemConfig, not hardcoded
    def __init__(self, rc: redis.Redis, cfg=None):
        self.rc  = rc
        self._cfg = cfg   # pass in or will lazy-load from Redis

    @property
    def cfg(self):
        from config.system_config import SystemConfig
        if self._cfg is None:
            self._cfg = SystemConfig.load(self.rc)
        return self._cfg

    def _reload_cfg(self):
        """Call at start of each promote cycle to pick up UI config changes."""
        from config.system_config import SystemConfig
        self._cfg = SystemConfig.load(self.rc)

    # ------------------------------------------------------------------ #
    #  Discovery — called by Reddit / StockTwits ingest loops             #
    # ------------------------------------------------------------------ #

    def propose(self, ticker: str, source: str) -> None:
        """Add a ticker to the candidate pool. Liquidity check runs async."""
        already_active    = self.rc.zscore(self.WATCHLIST_KEY, ticker) is not None
        already_candidate = self.rc.zscore(self.CANDIDATE_KEY, ticker) is not None
        is_seed           = self.rc.sismember(self.SEED_KEY, ticker)

        if already_active:
            self.touch(ticker)          # just refresh the TTL
            return
        if not already_candidate and not is_seed:
            self.rc.zadd(self.CANDIDATE_KEY, {ticker: time.time()})
            log.info("candidate +%s (source=%s)", ticker, source)

    def promote_candidates(self) -> None:
        """
        Run periodically (driven by cfg.watchlist_promote_interval).
        Reloads config first so UI changes to liquidity thresholds take effect.
        """
        self._reload_cfg()
        candidates = [t.decode() for t in self.rc.zrange(self.CANDIDATE_KEY, 0, -1)]
        seeds      = [t.decode() for t in self.rc.smembers(self.SEED_KEY)]

        for ticker in set(candidates + seeds):
            if self._passes_liquidity_gate(ticker):
                self.rc.zadd(self.WATCHLIST_KEY, {ticker: time.time()})
                self.rc.zrem(self.CANDIDATE_KEY, ticker)
                log.info("watchlist +%s  (promoted)", ticker)
            else:
                self.rc.zrem(self.CANDIDATE_KEY, ticker)
                log.debug("watchlist skip %s  (failed liquidity gate)", ticker)

    # ------------------------------------------------------------------ #
    #  Watchlist access — called by polling / signal loops                #
    # ------------------------------------------------------------------ #

    def get_active(self) -> list[str]:
        """Return current watchlist. Seeds always included."""
        active = [t.decode() for t in self.rc.zrange(self.WATCHLIST_KEY, 0, -1)]
        seeds  = [t.decode() for t in self.rc.smembers(self.SEED_KEY)]
        return list(set(active + seeds))

    def touch(self, ticker: str) -> None:
        """Refresh last-seen timestamp so the ticker isn't expired."""
        self.rc.zadd(self.WATCHLIST_KEY, {ticker: time.time()}, xx=True)

    def expire_stale(self) -> int:
        """Remove tickers not seen for cfg.watchlist_stale_hours. Seeds are never removed."""
        cutoff = time.time() - self.cfg.watchlist_stale_hours * 3600
        seeds  = {t.decode() for t in self.rc.smembers(self.SEED_KEY)}
        stale  = [t.decode() for t in
                  self.rc.zrangebyscore(self.WATCHLIST_KEY, 0, cutoff)]
        to_remove = [t for t in stale if t not in seeds]
        if to_remove:
            self.rc.zrem(self.WATCHLIST_KEY, *to_remove)
            log.info("watchlist expired: %s", to_remove)
        return len(to_remove)

    # ------------------------------------------------------------------ #
    #  Trader controls — called by Streamlit UI (§15)                     #
    # ------------------------------------------------------------------ #

    def pin(self, ticker: str) -> None:
        """Permanently add a ticker the trader always wants monitored."""
        self.rc.sadd(self.SEED_KEY, ticker)
        self.rc.zadd(self.WATCHLIST_KEY, {ticker: time.time()})

    def unpin(self, ticker: str) -> None:
        self.rc.srem(self.SEED_KEY, ticker)

    # ------------------------------------------------------------------ #
    #  Liquidity gate                                                      #
    # ------------------------------------------------------------------ #

    def _passes_liquidity_gate(self, ticker: str) -> bool:
        """
        Uses yfinance for free market data — only for watchlist admission.
        Thresholds come from SystemConfig (editable via Streamlit Config page).
        """
        try:
            info = yf.Ticker(ticker).fast_info
            avg_volume = getattr(info, "three_month_average_volume", 0) or 0
            last_price = getattr(info, "last_price", 0) or 0
            market_cap = getattr(info, "market_cap", 0) or 0
            adv_usd    = avg_volume * last_price

            if adv_usd    < self.cfg.watchlist_min_adv_usd:   return False
            if market_cap < self.cfg.watchlist_min_mcap_usd:  return False
            return True
        except Exception as e:
            log.warning("liquidity gate error for %s: %s", ticker, e)
            return False
```

**Typical watchlist size:** 15–40 active tickers on a normal day; spikes to 60–80 during
earnings season when Reddit volume surges. Redis handles this trivially.

**Seed list examples** (what the trader might pin permanently):
```python
wm = WatchlistManager(redis.Redis())
for ticker in ["SPY", "QQQ", "NVDA", "TSLA", "AAPL"]:
    wm.pin(ticker)
```

---

### 3b. X (Twitter) API v2

| Resource | Cost | When Used |
|----------|------|-----------|
| Post Read (Search/Stream) | $0.005 per post | Tier 2 only — on spike |
| Counts endpoint | **FREE** | Tier 1 — every 5 min |
| Trend Read | $0.010 per resource | Optional enrichment |

[^6]: docs.x.com/x-api/getting-started/pricing.md (verified)

#### Tier 1 — X Counts Polling (free)

Poll mention volume for every ticker in the active watchlist every 5 minutes.
The watchlist is read fresh each cycle so newly promoted tickers are picked up automatically.

```python
import requests, time, redis
from datetime import datetime, timedelta, timezone
from watchlist import WatchlistManager   # see §3a

BEARER_TOKEN = "YOUR_BEARER_TOKEN"
HEADERS      = {"Authorization": f"Bearer {BEARER_TOKEN}"}
COUNTS_URL   = "https://api.x.com/2/tweets/counts/recent"

rc = redis.Redis()
wm = WatchlistManager(rc)

def get_mention_count(ticker: str, window_minutes: int = 60) -> int:
    start = (datetime.now(timezone.utc) - timedelta(minutes=window_minutes)).isoformat()
    params = {
        "query":       f"${ticker} lang:en -is:retweet",
        "start_time":  start,
        "granularity": "minute",
    }
    r = requests.get(COUNTS_URL, headers=HEADERS, params=params)
    return r.json().get("meta", {}).get("total_tweet_count", 0)

def tier1_poll_loop():
    from config.system_config import SystemConfig
    while True:
        cfg = SystemConfig.load(rc)          # reload each cycle — picks up UI changes
        wm.expire_stale()
        wm.promote_candidates()

        for ticker in wm.get_active():       # dynamic — no hardcoded list
            count = get_mention_count(ticker, window_minutes=cfg.mention_window_minutes)
            if check_for_spike(ticker, count, zscore_threshold=cfg.spike_zscore_threshold):
                pull_spike_posts(ticker, max_results=cfg.x_search_max_results)
                wm.touch(ticker)

        time.sleep(cfg.counts_poll_interval_sec)
```

#### Tier 1 — Spike Detection (Z-score)

```python
import numpy as np
import redis

r = redis.Redis()

def check_for_spike(ticker: str, current_count: int, zscore_threshold: float = 2.0) -> bool:
    """
    Compare current count against 7-day rolling baseline stored in Redis.
    zscore_threshold sourced from SystemConfig.spike_zscore_threshold via the caller.
    """
    key = f"mention_history:{ticker}"
    history = [float(x) for x in r.lrange(key, 0, -1)]  # last 7 days of hourly counts

    if len(history) < 24:  # not enough history yet
        r.rpush(key, current_count)
        r.ltrim(key, -168, -1)  # keep 7 days × 24 hours = 168 data points
        return False

    mean = np.mean(history)
    std  = np.std(history) + 1e-9   # avoid div-by-zero on new tickers
    zscore = (current_count - mean) / std

    r.rpush(key, current_count)
    r.ltrim(key, -168, -1)

    return zscore >= zscore_threshold
```

#### Tier 2 — X Content Pull (triggered on spike)

Only called when `check_for_spike()` returns `True`. Pulls the 100 most recent posts
for the spiking ticker and pushes them to the Redis `raw_social` stream for NLP processing.

```python
import json

SEARCH_URL = "https://api.x.com/2/tweets/search/recent"

def pull_spike_posts(ticker: str, max_results: int = 100) -> list:
    """
    Costs: max_results × $0.005. Default = $0.50 per spike.
    Only called after spike detector fires.
    """
    params = {
        "query": f"${ticker} lang:en -is:retweet",
        "max_results": max_results,
        "tweet.fields": "created_at,entities,public_metrics,author_id",
        "expansions": "author_id",
        "user.fields": "verified,public_metrics",
    }
    r = requests.get(SEARCH_URL, headers=HEADERS, params=params)
    posts = r.json().get("data", [])

    # Push to Redis Streams for NLP pipeline
    rc = redis.Redis()
    for post in posts:
        rc.xadd("raw_social", {
            "source":  "twitter",
            "ticker":  ticker,
            "text":    post["text"],
            "post_id": post["id"],
            "metrics": json.dumps(post.get("public_metrics", {})),
            "ts":      post["created_at"],
        })
    return posts
```

**Useful query operators:**
```
followers_count:50000..    → High-influence accounts only
-is:retweet                → Original posts only (less noise)
$AAPL lang:en              → Cashtag + language filter
has:cashtags is:verified   → Verified accounts with any cashtag
```

**Rate limits:**
- Counts endpoint: 300 requests/15 min (Tier 1 polling)
- Search/recent: 450 requests/15 min, up to 100 results/request (Tier 2 pulls)

[^7]: docs.x.com/x-api/posts/filtered-stream/integrate/operators.md

---

### 3c. Reddit API (PRAW) — Discovery + Content (free)

Reddit serves double duty: it discovers new tickers **and** provides post content.
Every `$TICKER` regex match is proposed to the `WatchlistManager`; no prior knowledge needed.

```python
import praw, re, json, redis
from watchlist import WatchlistManager

reddit = praw.Reddit(
    client_id="YOUR_CLIENT_ID",
    client_secret="YOUR_CLIENT_SECRET",
    user_agent="fin-signal-bot:v1.0 (by /u/yourusername)"
)
rc = redis.Redis()
wm = WatchlistManager(rc)

WATCHED_SUBREDDITS = "wallstreetbets+stocks+options+investing"

for post in reddit.subreddit(WATCHED_SUBREDDITS).stream.submissions():
    tickers = re.findall(r'\$([A-Z]{1,5})\b', f"{post.title} {post.selftext}")
    if not tickers:
        continue

    for t in tickers:
        wm.propose(t, source="reddit")     # auto-discovery — no hardcoded list needed
        rc.incr(f"reddit_count_1h:{t}")
        rc.expire(f"reddit_count_1h:{t}", 3600)

    # Push content regardless — Reddit costs nothing
    rc.xadd("raw_social", {
        "source":       "reddit",
        "tickers":      json.dumps(tickers),
        "title":        post.title,
        "text":         post.selftext[:2000],
        "flair":        post.link_flair_text or "",
        "score":        post.score,
        "upvote_ratio": post.upvote_ratio,
        "num_comments": post.num_comments,
        "ts":           str(post.created_utc),
    })
```

**Key subreddits:** r/wallstreetbets (19.9M members), r/stocks, r/options, r/investing

**Flair weighting in signal scoring:**
| Flair | Signal Weight | Reason |
|-------|--------------|--------|
| `DD` (Due Diligence) | 1.5× | Researched thesis — highest predictive value |
| `YOLO` | 1.3× | High-conviction directional bet |
| `Gain` / `Loss` | 0.8× | Retrospective — price already moved |
| `Meme` / `Shitpost` | 0.3× | Noise |

[^8]: praw.readthedocs.io — verified API access; WSB subscriber count from reddit.com/r/wallstreetbets/about.json

---

### 3d. StockTwits API — Finance-Native Discovery + Sentiment (free)

StockTwits serves two roles: ticker discovery via the trending endpoint, and high-quality
directional signal via native Bullish/Bearish labels (no NLP needed for StockTwits posts).

```python
import requests, time, json, redis
from watchlist import WatchlistManager

BASE = "https://api.stocktwits.com/api/2"
rc   = redis.Redis()
wm   = WatchlistManager(rc)

def poll_stocktwits_trending(interval_seconds: int = 300) -> None:
    """Discover trending tickers and propose them to the watchlist."""
    while True:
        r = requests.get(f"{BASE}/streams/trending.json")
        for m in r.json().get("messages", []):
            for sym in m.get("symbols", []):
                wm.propose(sym["symbol"], source="stocktwits_trending")
        time.sleep(interval_seconds)

def poll_stocktwits_ticker(symbol: str) -> None:
    """Pull labelled messages for a known watchlist ticker."""
    r = requests.get(f"{BASE}/streams/symbol/{symbol}.json", params={"limit": 30})
    for m in r.json().get("messages", []):
        sentiment_label = (m.get("entities", {})
                            .get("sentiment", {})
                            .get("basic"))    # "Bullish", "Bearish", or None
        rc.xadd("raw_social", {
            "source":          "stocktwits",
            "ticker":          symbol,
            "text":            m["body"],
            "sentiment_label": sentiment_label or "neutral",  # skip NLP — use directly
            "likes":           m.get("likes", {}).get("total", 0),
            "user_followers":  m["user"]["followers_count"],
            "ts":              m["created_at"],
        })
        wm.touch(symbol)    # keep ticker alive in watchlist
```

**Why StockTwits sentiment labels matter:** Unlike Twitter/Reddit where you must infer
direction from NLP, StockTwits users explicitly tag posts as Bullish or Bearish. This
gives a direct, low-noise directional signal usable as a standalone factor.

**Pricing:**
| Tier | Price | Use Case |
|------|-------|----------|
| Free | $0 | Public symbol streams, trending — sufficient for this system |
| Edge | $229.50/yr | Expanded sentiment data, higher rate limits |
| Enterprise | Custom | Full firehose access |

**Rate limit (free tier):** ~200 requests/hour — poll 20 tickers every 6 minutes comfortably.

---

### 3e. Daily Cost Summary

Assumptions: dynamic watchlist averages ~25 active tickers (15 seeds + ~10 discovered),
market hours 8:00 AM–4:30 PM ET (~8.5 hrs), ~5 genuine spike events per day (Z-score ≥ 2.0),
100 posts pulled per X spike. yfinance liquidity checks: ~30 candidate checks/day.

| Source | Activity | Unit Cost | Daily Volume | Daily Cost |
|--------|----------|-----------|-------------|------------|
| X Counts polls | 25 tickers × every 5 min × 8.5 hrs | FREE | 2,550 polls | **$0** |
| X Search pulls | 5 spikes × 100 posts | $0.005/post | 500 posts | **$2.50** |
| Reddit PRAW stream | Continuous discovery + content | FREE | unlimited | **$0** |
| StockTwits trending | Every 5 min | FREE | 102 polls | **$0** |
| StockTwits symbol | 25 tickers × every 6 min × 8.5 hrs | FREE | 2,125 polls | **$0** |
| yfinance liquidity | ~30 candidate checks/day | FREE | 30 requests | **$0** |
| **Total** | | | | **~$2.50/day** |

**Monthly cost (22 trading days):** ~$55 typical / ~$110 on high-volatility months.

> **No static ticker list required.** The watchlist self-populates from Reddit and StockTwits.
> Seed a handful of preferred tickers via `wm.pin()` and the rest is automatic.

---

*[⬆ Back to main index](README.md)*
