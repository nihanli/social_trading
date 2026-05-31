## 4. NLP Pipeline

### 4a. Processing Pipeline Overview

```
Raw Post Text
    ↓
[1] Bot Filter (account age, velocity, follower ratio)
    ↓
[2] Ticker Extraction (cashtag regex + spaCy NER)
    ↓
[3] VADER Pre-filter (< 30ms; drop neutral; flag extreme)
    ↓
[4] FinBERT-Tone Classification (GPU; 3-class probabilities)
    ↓
[5] Engagement Weighting (likes, followers, time decay)
    ↓
[6] Time-Bucket Aggregation (15-minute windows)
    ↓
Ticker-level Sentiment Signal
    ↓
[7] Signal Service — Two-Phase Evaluation (§5a)
      Phase 1: free sources → signal_phase1_threshold
        └─ Tier-2 enrichment requested (if X/Twitter API enabled)
      Phase 2: +Tier-2 data → signal_phase2_threshold → fire signal
```

**Tier-2 enrichment flow:**  When a ticker's aggregated sentiment (from free sources)
passes the Phase 1 quality threshold, the signal service publishes a request to
`enrichment:requests`.  The ingest service consumes this stream, calls the X/Twitter
API for that specific ticker, and publishes any returned posts back through the normal
`raw_social → NLP → sentiment_signals → aggregator` pipeline.  On the next signal
evaluation cycle, the aggregator window for that ticker now includes Twitter data, so
Phase 2 evaluation fires.

### 4b. Recommended Models

| Model | Use Case | Speed | Accuracy |
|-------|---------|-------|---------|
| VADER | Pre-filter, volume detection | ⚡⚡⚡ <1ms | ⭐⭐ |
| FinBERT-Tone (`yiyanghkust/finbert-tone`) | Primary classifier | ⚡⚡ ~30ms/GPU | ⭐⭐⭐⭐⭐ |
| FinGPT (LLaMA LoRA, AI4Finance-Foundation) | Complex analysis, low volume | ⚡ ~200ms | ⭐⭐⭐⭐⭐ |
| GPT-4 API | Deep analysis for important signals | ⚡ slow | ⭐⭐⭐⭐⭐ |

[^9]: Huang, Wang & Yang (2022). FinBERT: A Large Language Model for Extracting Information from Financial Text. *Contemporary Accounting Research*. HuggingFace: yiyanghkust/finbert-tone

### 4c. FinBERT-Tone Implementation

```python
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch, numpy as np

tokenizer = AutoTokenizer.from_pretrained("yiyanghkust/finbert-tone")
model = AutoModelForSequenceClassification.from_pretrained("yiyanghkust/finbert-tone")
model.eval()

def classify_sentiment(text: str) -> dict:
    inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=512)
    with torch.no_grad():
        outputs = model(**inputs)
    probs = torch.nn.functional.softmax(outputs.logits, dim=-1).numpy()[0]
    labels = ["negative", "neutral", "positive"]
    return {
        "neg_prob": float(probs[0]),
        "neu_prob": float(probs[1]),
        "pos_prob": float(probs[2]),
        "label": labels[np.argmax(probs)],
        "score": float(probs[np.argmax(probs)])  # confidence
    }
```

[^10]: HemantBK/Algorithmic-Trading-AI:src/Stocex.py:85-115 (verified code pattern)

### 4d. Ticker Extraction (Multi-Pass)

```python
import re, spacy

nlp = spacy.load("en_core_web_sm")

def extract_tickers_multi_pass(text: str, valid_tickers: set) -> set:
    tickers = set()
    
    # Pass 1: Cashtags — highest precision
    cashtags = re.findall(r'\$([A-Z]{1,5})(?!\w)', text.upper())
    tickers.update(t for t in cashtags if t in valid_tickers)
    
    # Pass 2: spaCy ORG entities mapped to known ticker universe
    doc = nlp(text)
    for ent in doc.ents:
        if ent.label_ == "ORG":
            match = company_to_ticker.get(ent.text.lower())
            if match:
                tickers.add(match)
    
    # Pass 3: Standalone uppercase 2-5 char words in known universe
    words = re.findall(r'\b([A-Z]{2,5})\b', text)
    tickers.update(w for w in words if w in valid_tickers)
    
    return tickers
```

[^11]: HemantBK/Algorithmic-Trading-AI:src/Stocex.py:50-92 (verified)

### 4e. Bot Detection

```python
class BotFilter:
    def is_bot(self, user: dict) -> bool:
        age_days = (datetime.now() - user['created_at']).days
        if age_days < 30:
            return True  # Too new
        if user['followers'] > 0:
            ratio = user['following'] / user['followers']
            if ratio > 10:   # Following 10x more than followers
                return True
        # > 100 tweets/day = likely automated
        if user.get('statuses_count', 0) / max(age_days, 1) > 100:
            return True
        return False
```

### 4f. Engagement-Weighted Signal Aggregation

```python
import math
from datetime import datetime

DECAY_LAMBDA = 0.1  # Signal half-life ≈ 7 hours

def compute_weighted_sentiment(posts: list) -> float:
    """
    Combine: engagement (likes/retweets), authority (follower count),
    and time decay into a single weighted sentiment score per ticker.
    """
    weighted_sum = total_weight = 0.0
    for post in posts:
        # Engagement: retweets spread more than likes
        eng = math.log1p(post['likes'] + 2*post['retweets'] + 3*post['replies'])
        # Authority: log scale prevents single whale dominating
        auth = math.log1p(post['follower_count'])
        # Time decay: exponential
        hours_ago = (datetime.utcnow() - post['created_at']).total_seconds() / 3600
        time_w = math.exp(-DECAY_LAMBDA * hours_ago)
        
        weight = eng * auth * time_w
        sentiment = post['pos_prob'] - post['neg_prob']  # FinBERT output
        weighted_sum += sentiment * weight
        total_weight += weight
    
    return weighted_sum / total_weight if total_weight > 0 else 0.0
```

[^12]: Research synthesis from PSURI1894/stock_sentiment_realtime:sentiment_utils.py and FinBERT literature

---

---

*[⬆ Back to main index](README.md)*
