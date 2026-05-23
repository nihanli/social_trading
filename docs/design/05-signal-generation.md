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

---

*[⬆ Back to main index](README.md)*
