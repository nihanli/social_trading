## 5. Signal Generation Logic

### 5a. Main Signal Loop

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
        for ticker in wm.get_active():
            result = compute_trading_signal(ticker, cfg=cfg)
            if result["signal"] != "FLAT":
                rc.xadd("strategy_signals", {
                    "ticker":  ticker,
                    "signal":  result["signal"],
                    "score":   result["score"],
                    "ts":      str(time.time()),
                })
        time.sleep(cfg.signal_poll_interval_sec)
```

### 5b. Signal Construction Pipeline

```python
import pandas as pd
import numpy as np

def compute_trading_signal(ticker: str, window_hours: int = 24,
                            cfg=None) -> dict:
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

    # 7. Signal decision — threshold from SystemConfig
    if quality_score >= cfg.signal_quality_threshold and sentiment_score > cfg.sentiment_strength_min and price_momentum >= 0:
        signal = "LONG"
    elif quality_score >= cfg.signal_quality_threshold and sentiment_score < -cfg.sentiment_strength_min and price_momentum <= 0:
        signal = "SHORT"
    else:
        signal = "FLAT"
    
    return {"signal": signal, "score": quality_score, "sentiment": sentiment_score,
            "mention_zscore": mention_zscore, "ticker": ticker}
```

[^13]: Synthesis of SpotDylan/SocialMediaTradeBotV1, galafis/rust-sentiment-analysis-trading, and Buz & de Melo (2021) signal quality framework

### 5c. Signal Decay — Time-Limit Enforcement

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

### 5d. Quality Score — Implementation Reference

The `SignalGenerator` class (`src/social_trading/signals/generator.py`) implements the
quality formula from §5b with the following exact computation and default weights.

#### Formula

```
quality = (w_volume × v  +  w_sentiment × s  +  w_proactivity × p
           +  w_momentum × m  +  w_convergence × c)
          ÷ sum_of_active_weights
```

The raw weighted sum is **normalised by the sum of active weights** so that factors that
are unavailable (e.g. `price_momentum = 0.0` before the market-data service is live) do
not permanently lower the score ceiling.  A signal fires when:

- `quality ≥ signal_quality_threshold` (default **0.50**)
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
score in the **0.50–0.65** range.  Enabling Phase 5 market data raises the ceiling
toward `1.0` by contributing the `w_momentum = 0.15` term.

#### Configuration (all tunable via UI → Config page)

| Parameter | Default | Effect |
|-----------|---------|--------|
| `signal_quality_threshold` | 0.50 | Minimum quality to fire a signal |
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
