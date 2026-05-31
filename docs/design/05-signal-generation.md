## 5. Signal Generation Logic

### 5a. Two-Phase Signal Pipeline

The signal engine implements a **two-phase quality gate** to balance signal coverage
(using always-free data) with signal precision (confirmed by metered paid data):

```
Phase 1 — Free/Tier-1 sources only (Bluesky, Reddit, StockTwits, YFinance)
    ↓
  quality ≥ signal_phase1_threshold (default 0.40)?
    │
    ├─ YES + no Tier-2 API configured ──► fire signal directly → strategy_signals
    │
    └─ YES + Tier-2 (X/Twitter) enabled ──► publish to enrichment:requests
                                                    ↓
                                         Enrichment Loop calls Twitter for this ticker
                                                    ↓
                                         Phase 2 — all sources in aggregator window
                                                    ↓
                                          quality ≥ signal_phase2_threshold (default 0.65)?
                                            ├─ YES ──► fire signal → strategy_signals
                                            └─ NO  ──► suppress (Tier-2 confirmed weak)
```

**Key properties:**
- Phase 1 and Phase 2 thresholds are fully independent — no ordering constraint.
- A ticker that scores ≥ phase1_threshold fires directly when no paid API is configured,
  so the system always produces signals even without an X/Twitter subscription.
- Enrichment is deduplicated per cycle via `enrichment:sent:{ticker}` (TTL = poll interval).
- Open positions can be skipped from enrichment to avoid paying for redundant data
  (`phase2_skip_open_positions`, default True).
- At most `phase2_max_tickers_per_cycle` (default 10) Tier-2 calls per signal cycle
  to cap API costs.

### 5b. Main Signal Loop

The signal engine reads the active watchlist from Redis (populated by the discovery layer
in §3a) and evaluates each ticker every cycle. No ticker list is hardcoded.

```python
import time, redis
from watchlist import WatchlistManager   # §3a

rc = redis.Redis()
wm = WatchlistManager(rc)

def signal_engine_loop():
    from config.system_config import SystemConfig
    while True:
        cfg = SystemConfig.load(rc)          # reload each cycle — picks up UI changes
        tier2_active = cfg.x_api_enabled

        for ticker in wm.get_active():
            stats = aggregator.get_stats(ticker)
            has_tier2_data = bool(stats.sources & {"twitter"})

            if has_tier2_data:
                # Phase 2: Tier-2 data present — apply higher threshold
                sig = generator.evaluate(stats, cfg,
                                         quality_threshold=cfg.signal_phase2_threshold)
                if sig:
                    rc.xadd("strategy_signals", serialise(sig))
            else:
                # Phase 1: free sources only
                sig = generator.evaluate(stats, cfg,
                                         quality_threshold=cfg.signal_phase1_threshold)
                if sig:
                    if not tier2_active:
                        rc.xadd("strategy_signals", serialise(sig))   # direct fire
                    else:
                        rc.xadd("enrichment:requests", {"ticker": ticker, ...})  # request enrichment

        time.sleep(cfg.signal_poll_interval_sec)
```

### 5c. Signal Construction Pipeline

```python
import pandas as pd
import numpy as np

def compute_trading_signal(ticker: str, window_hours: int = 24,
                            cfg=None, quality_threshold: float = 0.40) -> dict:
    """Single-ticker multi-factor signal. All thresholds from SystemConfig."""
    import redis
    from config.system_config import SystemConfig
    cfg = cfg or SystemConfig.load(redis.Redis())
    # 1. Volume anomaly: Z-score of mention count vs rolling baseline
    current_mentions = get_mention_count(ticker, last_hours=1)
    baseline_mean, baseline_std = get_mention_baseline(ticker, days=30)
    mention_zscore = (current_mentions - baseline_mean) / (baseline_std + 1e-9)
    
    # 2. Sentiment signal: weighted aggregate
    sentiment_score = compute_weighted_sentiment(get_recent_posts(ticker, hours=window_hours))
    
    # 3. Price momentum confirmation
    price_return_24h = get_price_return(ticker, hours=24)
    price_momentum = np.sign(price_return_24h) if abs(price_return_24h) > 0.02 else 0
    
    # 4. Proactivity filter: is price already up before mention spike? (reactive = noise)
    price_before_spike = get_price_return(ticker, hours=-24)  # Price BEFORE mentions
    is_reactive = abs(price_before_spike) > cfg.reactive_price_threshold

    # 5. Cross-platform convergence (Twitter + Reddit both showing signal = stronger)
    twitter_signal = get_platform_sentiment(ticker, "twitter")
    reddit_signal  = get_platform_sentiment(ticker, "reddit")
    convergence_bonus = cfg.convergence_bonus if (twitter_signal * reddit_signal > 0) else 0.0

    # 6. Signal quality composite score (weights from SystemConfig)
    quality_score = (
        cfg.w_volume      * min(mention_zscore / 3.0, 1.0) +
        cfg.w_sentiment   * abs(sentiment_score) +
        cfg.w_proactivity * (1 - is_reactive) +
        cfg.w_momentum    * abs(price_momentum) +
        cfg.w_convergence * convergence_bonus
    )

    # 7. Signal decision — threshold passed in (phase1 or phase2 depending on caller)
    if quality_score >= quality_threshold and sentiment_score > cfg.sentiment_strength_min and price_momentum >= 0:
        signal = "LONG"
    elif quality_score >= quality_threshold and sentiment_score < -cfg.sentiment_strength_min and price_momentum <= 0:
        signal = "SHORT"
    else:
        signal = "FLAT"
    
    return {"signal": signal, "score": quality_score, "sentiment": sentiment_score,
            "mention_zscore": mention_zscore, "ticker": ticker}
```

[^13]: Synthesis of SpotDylan/SocialMediaTradeBotV1, galafis/rust-sentiment-analysis-trading, and Buz & de Melo (2021) signal quality framework

### 5d. Signal Decay — Time-Limit Enforcement

```python
import math
from config.system_config import SystemConfig
import redis

def apply_signal_decay(raw_signal: float, hours_since_detection: float,
                       cfg: SystemConfig = None) -> float:
    """Apply hyperbolic decay to signal strength. λ from SystemConfig."""
    cfg = cfg or SystemConfig.load(redis.Redis())
    return raw_signal * math.exp(-cfg.signal_decay_lambda * hours_since_detection)

def is_signal_expired(hours_since_detection: float, cfg: SystemConfig = None) -> bool:
    cfg = cfg or SystemConfig.load(redis.Redis())
    return hours_since_detection > cfg.signal_age_max_hours
```

---

### 5e. Quality Score — Implementation Reference

The `SignalGenerator` class (`src/social_trading/signals/generator.py`) implements the
quality formula from §5c with the following exact computation and default weights.

#### Formula

```
quality = (w_volume × v  +  w_sentiment × s  +  w_proactivity × p
           +  w_momentum × m  +  w_convergence × c)
          ÷ sum_of_active_weights
```

The raw weighted sum is **normalised by the sum of active weights** so that factors that
are unavailable (e.g. `price_momentum = 0.0` before the market-data service is live) do
not permanently lower the score ceiling.  A signal fires when:

- `quality ≥ quality_threshold` (Phase 1: `signal_phase1_threshold` default **0.40**;
   Phase 2: `signal_phase2_threshold` default **0.65**)
- `|mean_sentiment| ≥ sentiment_strength_min` (default **0.30**)
- If market data is available: price direction must not strongly contradict sentiment

#### The 5 Factors

| Factor | Weight | Symbol | Range | Description |
|--------|--------|--------|-------|-------------|
| Volume Z-score | **0.30** | `v` | 0–1 | Abnormality of current mention volume vs 7-day baseline |
| Sentiment strength | **0.25** | `s` | 0–1 | Magnitude of mean NLP sentiment score across the window |
| Proactivity | **0.20** | `p` | 0 or 1 | Whether chatter **led** the price move (1) or merely reacted (0) |
| Price momentum | **0.15** | `m` | 0–1 | Price movement confirming the direction |
| Convergence | **0.10** | `c` | 0–0.20 | Cross-platform agreement, scaled by `convergence_bonus` cap |

**`v` — Volume Z-score** (weight 0.30, highest)
```
v = min(mention_zscore / 3.0, 1.0)
```
The rolling mention count for the ticker is compared against its 7-day hourly baseline
(mean ± std).  A Z-score ≥ 3.0 scores a full `1.0`.  Captures *unusual* activity, not
just high absolute volume.

**`s` — Sentiment Strength** (weight 0.25)
```
s = min(|mean_score|, 1.0)
```
The NLP model assigns each post a signed score in `[−1, 1]`.  The window mean is taken
across all posts; its absolute value is `s`.  The sign determines direction (LONG/SHORT);
the magnitude is the quality contribution.

**`p` — Proactivity** (weight 0.20)
```
p = 0.0 if is_reactive else 1.0
```
If the price moved significantly *before* the mention spike, the social activity is
likely noise (the crowd reacting to price, not leading it).  The `is_reactive` flag is
set by comparing price returns in the hours preceding vs following the spike.
Currently `1.0` until the market-data service (Phase 5) is live.

**`m` — Price Momentum** (weight 0.15)
```
m = min(|price_change| / 0.10, 1.0)
```
Confirms that price is already moving in the same direction as sentiment.  A 10% move
scores a full `1.0`.  When `price_momentum = 0.0` (no market data), this factor is
excluded from the denominator of the normalisation so the score ceiling is not lowered.
Currently `0.0` until the market-data service (Phase 5) is live.

**`c` — Convergence** (weight 0.10, lowest)
```
c = (agreeing_platforms / total_platforms) × convergence_bonus
```
Counts how many social platforms agree on the signal direction, scaled by
`convergence_bonus` (default **0.20**).  Maximum possible value is `0.20` (all platforms
agree).  With a single active source (e.g. only Bluesky), the single source always
"agrees" with itself → `c = 1.0 × 0.20 = 0.20`.

#### Effective Score Ceiling

Without price momentum (Phase 5 not yet live), the normalisation denominator is `0.85`
(excludes `w_momentum = 0.15`).  Maximum possible quality in this state:

```
max_quality = (0.30×1.0 + 0.25×1.0 + 0.20×1.0 + 0.10×0.20) / 0.85 ≈ 0.906
```

With multiple active platforms adding convergence and a volume spike, typical signals
score in the **0.45–0.65** range.  Phase 1 signals (free sources only) typically land
between 0.40 and 0.60; Tier-2 enrichment from X/Twitter can push scores above the 0.65
Phase 2 threshold when cross-platform convergence confirms the move.

#### Configuration (all tunable via UI → Config page)

| Parameter | Default | Effect |
|-----------|---------|--------|
| `signal_phase1_threshold` | 0.40 | Phase 1 gate — free sources only; passing tickers request Tier-2 enrichment (or fire directly if no Tier-2 configured) |
| `signal_phase2_threshold` | 0.65 | Phase 2 gate — all sources required; passing tickers fire to execution |
| `phase2_max_tickers_per_cycle` | 10 | Max Tier-2 API calls per signal evaluation cycle (cost cap) |
| `phase2_skip_open_positions` | True | Skip enrichment when position already open for ticker |
| `sentiment_strength_min` | 0.30 | Minimum \|mean_score\| for LONG/SHORT direction |
| `convergence_bonus` | 0.20 | Cap on the convergence factor `c` |
| `w_volume` | 0.30 | Weight for mention volume Z-score |
| `w_sentiment` | 0.25 | Weight for sentiment strength |
| `w_proactivity` | 0.20 | Weight for proactivity flag |
| `w_momentum` | 0.15 | Weight for price momentum |
| `w_convergence` | 0.10 | Weight for cross-platform convergence |

---

---

*[⬆ Back to main index](README.md)*
