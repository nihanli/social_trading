## 13. Monitoring & Observability

### Prometheus Metrics (expose from each service)

```python
from prometheus_client import Counter, Gauge, Histogram, start_http_server

# Signal metrics
signals_generated = Counter("signals_total", "Signals generated", ["ticker", "direction"])
signal_quality    = Histogram("signal_quality_score", "Signal quality distribution",
                              buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9])

# Execution metrics
orders_placed  = Counter("orders_total", "Orders placed", ["ticker", "status"])
position_pnl   = Gauge("position_unrealized_pnl", "Unrealized P&L", ["ticker"])
portfolio_heat = Gauge("portfolio_heat", "Portfolio volatility-adjusted exposure")

# Social media ingestion metrics
posts_ingested  = Counter("posts_ingested_total", "Posts ingested", ["source", "ticker"])
posts_filtered  = Counter("posts_filtered_total", "Posts filtered (bots/noise)", ["reason"])
sentiment_latency = Histogram("sentiment_latency_ms", "NLP processing latency")

# P&L metrics
paper_equity    = Gauge("paper_equity_usd", "Paper trading equity")
daily_pnl       = Gauge("daily_pnl_pct", "Daily P&L as % of portfolio")
drawdown        = Gauge("drawdown_from_hwm", "Current drawdown from high water mark")

# Start metrics server on each microservice
start_http_server(8090)  # Scraped by Prometheus every 15 seconds
```

### Alerting Rules

```yaml
# Prometheus alerting rules
groups:
  - name: social_trading
    rules:
      - alert: DailyLossLimitBreach
        expr: daily_pnl_pct < -0.03
        severity: critical
        annotations:
          summary: "Daily loss limit breached ({{ $value | humanizePercentage }})"
      
      - alert: DrawdownWarning
        expr: drawdown_from_hwm > 0.10
        severity: warning
      
      - alert: IBKRDisconnected
        expr: up{job="ibkr_connection"} == 0
        for: 2m
        severity: critical
      
      - alert: SentimentLatencyHigh
        expr: histogram_quantile(0.99, sentiment_latency_ms) > 500
        severity: warning
```

[^27]: ashwini-singhh/crypto_trading_agent:python-services/strategy-engine/main.py:25-32 (verified Prometheus pattern); alosti/maotrade-fintech-showcase:README.md:79-92 (Nagios alerting tiers)

---

---

*[⬆ Back to main index](README.md)*
