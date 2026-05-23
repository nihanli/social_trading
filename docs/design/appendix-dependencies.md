## Appendix: Recommended Python Dependencies

```txt
# Social Media APIs
tweepy>=4.14.0          # X/Twitter API v2 streaming
praw>=7.7.0             # Reddit API

# NLP
transformers>=4.35.0    # FinBERT-Tone model
torch>=2.0.0            # PyTorch for FinBERT inference
vaderSentiment>=3.3.2   # Fast pre-filter
spacy>=3.7.0            # NER for ticker extraction

# Broker / Execution
ib_async>=0.9.0         # Interactive Brokers API

# Data & Analysis
pandas>=2.0.0
numpy>=1.24.0
scipy>=1.11.0

# Infrastructure
redis>=4.6.0            # Redis Streams
psycopg2-binary>=2.9.0  # PostgreSQL
prometheus_client>=0.17.0  # Metrics

# Alternative Data
sanpy>=0.12.0           # Santiment API
pytrends>=4.9.0         # Google Trends

# Market Calendar
exchange_calendars>=4.3.0   # NYSE/NASDAQ holiday handling

# Backtesting / Research
vectorbt>=0.26.0        # Fast backtesting grid search

# UI — Monitoring (§15)
streamlit>=1.32.0       # Ops control panel
plotly>=5.18.0          # Charts in Streamlit
grafana/grafana:latest  # Docker image — dashboards + alerting
prom/prometheus:latest  # Docker image — metrics scraping
```

---

---

---

*[⬆ Back to main index](README.md)*
