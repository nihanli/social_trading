## 8. Infrastructure & Technology Stack

### Technology Choices

| Layer | Technology | Rationale |
|-------|-----------|-----------|
| Social API ingest | Python + asyncio + tweepy + praw | Non-blocking I/O for streaming |
| NLP sentiment | HuggingFace transformers (FinBERT-Tone) | Finance-domain pre-trained |
| Event bus | Redis Streams (XADD/XREAD with consumer groups) | Production-proven; sufficient for single-node; no Kafka overhead |
| Strategy engine | Python + Pandas + NumPy | Flexibility; rich financial libraries |
| Price feed | ib_async `reqMktData` | Direct IBKR integration |
| Risk manager | Python microservice | Isolated; can be upgraded without touching execution |
| Primary storage | PostgreSQL 15 | ACID; time-series indexes; signals + trades + equity |
| Log aggregation | Fluentd → MongoDB | Structured logs with per-strategy filtering |
| Metrics | Prometheus + Grafana | Real-time dashboards; latency/throughput monitoring |
| Containerization | Docker Compose (dev) / Docker Swarm (prod) | Reproducible; isolated secrets |
| Market calendar | `exchange_calendars` Python library | NYSE/NASDAQ holiday handling |

[^20]: ashwini-singhh/crypto_trading_agent:infrastructure/docker-compose.yml (verified architecture); alosti/maotrade-fintech-showcase (verified monitoring pattern)

### Event Bus Pattern (Redis Streams)

```python
import redis

class TradingEventBus:
    STREAMS = {
        "raw_social":   "raw_social_stream",
        "sentiment":    "sentiment_signals_stream",
        "market_data":  "market_data_stream",
        "strategy":     "strategy_signals_stream",
        "selected":     "selected_signals_stream",
    }
    
    def __init__(self):
        self.redis = redis.Redis(host='localhost', port=6379, decode_responses=True)
    
    def publish(self, stream_key: str, data: dict, maxlen: int = 10_000):
        """Publish event to Redis Stream with automatic trimming."""
        self.redis.xadd(self.STREAMS[stream_key], data, maxlen=maxlen)
    
    def consume(self, stream_key: str, consumer_group: str, 
                consumer_name: str, block_ms: int = 2000):
        """Consume events as part of a consumer group (allows parallel workers)."""
        stream = self.STREAMS[stream_key]
        try:
            results = self.redis.xreadgroup(
                consumer_group, consumer_name,
                {stream: ">"}, count=100, block=block_ms
            )
            return results
        except redis.ConnectionError as e:
            # Auto-reconnect on disconnect
            self.redis = redis.Redis(host='localhost', port=6379, decode_responses=True)
            return []
```

---

---

*[⬆ Back to main index](README.md)*
