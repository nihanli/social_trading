## 11. Alternative Data Sources

### 11a. LunarCrush (Best for Crypto Social Signals)

```python
import requests

LUNAR_API_KEY = "your_key_here"
BASE = "https://lunarcrush.com/api4/public"
HEADERS = {"Authorization": f"Bearer {LUNAR_API_KEY}"}

def get_coin_social_snapshot(symbol: str) -> dict:
    """Galaxy Score (0-100 composite) + AltRank (momentum vs own history)."""
    r = requests.get(f"{BASE}/coins/{symbol}/v1", headers=HEADERS)
    data = r.json()["data"]
    return {
        "galaxy_score":    data["galaxy_score"],      # 0-100 composite
        "alt_rank":        data["alt_rank"],           # lower = better momentum
        "social_volume":   data["social_volume_24h"],
        "sentiment":       data["average_sentiment"],  # 1-5 scale
        "interactions_24h": data["interactions_24h"],
    }
```

**AltRank Signal Logic:** Low AltRank (1–10) = asset is dramatically outperforming its own social history → strong momentum signal. Galaxy Score > 75 = overall healthy social + market momentum.

**Pricing:** Free (~100 calls/day), Pro (~10,000 calls/day)

[^23]: LunarCrush API v4 docs at lunarcrush.com/api4; saizk/LunarCrushAPI:lunarcrush/lcv3.py:60-175

### 11b. Santiment (On-Chain + Social Combined)

```python
import san
san.ApiConfig.api_key = "your_santiment_api_key"

# Social Volume (updated every 5 minutes)
social_vol = san.get("social_volume_total", slug="bitcoin",
                     from_date="2024-01-01", to_date="2024-01-31", interval="1h")

# Social Dominance (% of all crypto discussion)
dominance = san.get("social_dominance_total", slug="ethereum",
                     from_date="2024-01-01", to_date="2024-01-31", interval="1d")
```

**Sources covered:** 4chan, telegram, reddit, twitter, bitcointalk, youtube, farcaster.

**Pricing:** Free (1K calls/month, 30-day data lag), Pro $49/mo, Max $249/mo (real-time).[^24]

[^24]: santiment.net/pricing (verified); academy.santiment.net/metrics/social-volume/ (metric definition)

### 11c. Fear & Greed Index (Free Macro Filter)

```python
def get_fear_greed_index(days: int = 30) -> pd.DataFrame:
    url = f"https://api.alternative.me/fng/?limit={days}&date_format=us"
    data = requests.get(url).json()["data"]
    df = pd.DataFrame(data)
    df["value"] = df["value"].astype(int)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
    return df.set_index("timestamp").sort_index()
```

**Current value (live):** Index = 29 ("Fear") at time of research.

**Signal use:** Extreme Fear (0–20) → scale up social momentum longs; Extreme Greed (80–100) → scale down / take profits. Use as a portfolio-level multiplier, not a standalone signal.[^25]

[^25]: alternative.me/crypto/fear-and-greed-index/ — verified API response: `{"value":"29","value_classification":"Fear"}`

### 11d. Quiver Quantitative (WSB Reddit Mentions Data)

- Tracks WallStreetBets mentions and Congressional trades
- API endpoint: `api.quiverquant.com/beta/live/wallstreetbets` (subscription required)
- Also provides: Insider trading, government contracts, patent filings
- **Use case:** Cross-validate Reddit mentions against Quiver's pre-processed data

---

---

*[⬆ Back to main index](README.md)*
