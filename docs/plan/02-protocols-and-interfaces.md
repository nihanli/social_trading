# 02 — Protocols and Interfaces

All cross-component contracts are defined in `src/social_trading/core/protocols.py`.
Components depend on these abstractions — never on each other's concrete classes.
This enables parallel development and easy swapping of implementations in tests.

---

## Core Data Models (`core/models.py`)

```python
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal
from pydantic import BaseModel, Field


class SocialPost(BaseModel):
    """Normalised social media post from any data source."""
    id: str
    source: Literal["twitter", "reddit", "stocktwits", "lunarcrush"]
    ticker: str
    text: str
    author_id: str
    author_followers: int = 0
    author_account_age_days: int = 0
    likes: int = 0
    reposts: int = 0
    collected_at: datetime = Field(default_factory=datetime.utcnow)
    raw: dict = Field(default_factory=dict)   # source-specific payload


class SentimentResult(BaseModel):
    """Output of any SentimentClassifier."""
    post_id: str
    ticker: str
    positive: float           # probability [0,1]
    negative: float
    neutral: float
    score: float              # positive - negative ∈ [-1, 1]
    model: str                # "vader" | "finbert"
    latency_ms: float


class Signal(BaseModel):
    """Strategy signal ready for risk review."""
    ticker: str
    direction: Literal["LONG", "SHORT"]
    quality_score: float      # 0–1 composite (see design §5)
    sentiment_score: float
    volume_z_score: float
    momentum: float
    convergence: float        # fraction of sources agreeing
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    source_post_count: int
    metadata: dict = Field(default_factory=dict)


class OrderResult(BaseModel):
    """Result of an execution engine order."""
    order_id: str
    ticker: str
    direction: Literal["LONG", "SHORT"]
    quantity: int
    fill_price: float | None
    status: Literal["submitted", "filled", "rejected", "cancelled"]
    submitted_at: datetime = Field(default_factory=datetime.utcnow)
    error: str | None = None


@dataclass
class Position:
    ticker: str
    direction: Literal["LONG", "SHORT"]
    quantity: int
    entry_price: float
    entry_time: datetime
    stop_loss: float
    take_profit: float
    unrealised_pnl: float = 0.0
    metadata: dict = field(default_factory=dict)


@dataclass
class AccountState:
    net_liquidation: float
    cash: float
    daily_pnl: float
    weekly_pnl: float
    drawdown_pct: float       # from peak
    open_positions: list[Position] = field(default_factory=list)
```

---

## Protocol Definitions (`core/protocols.py`)

Python's `typing.Protocol` enables **structural subtyping** — any class implementing
the right methods satisfies the protocol, no inheritance required.

```python
from __future__ import annotations
from typing import Protocol, AsyncIterator, runtime_checkable
from .models import SocialPost, SentimentResult, Signal, OrderResult, Position, AccountState


# ─────────────────────────────────────────────
#  1. DATA SOURCE — ingest layer
# ─────────────────────────────────────────────

@runtime_checkable
class DataSource(Protocol):
    """
    Any social media data source must satisfy this protocol.
    Implementations: TwitterDataSource, RedditDataSource, etc.
    """
    name: str                           # "twitter" | "reddit" | "stocktwits"
    is_streaming: bool                  # True = push; False = polled

    async def stream(self) -> AsyncIterator[SocialPost]:
        """Yield posts as they arrive (streaming sources)."""
        ...

    async def poll(self, tickers: list[str]) -> list[SocialPost]:
        """Fetch recent posts for given tickers (polling sources)."""
        ...

    async def get_trending(self) -> list[str]:
        """Return list of trending tickers on this platform."""
        ...

    async def health_check(self) -> bool:
        """Return True if the API is reachable and authenticated."""
        ...


# ─────────────────────────────────────────────
#  2. SENTIMENT CLASSIFIER — NLP layer
# ─────────────────────────────────────────────

@runtime_checkable
class SentimentClassifier(Protocol):
    """
    Any sentiment model must satisfy this protocol.
    Implementations: VaderClassifier, FinBERTClassifier.
    """
    model_name: str

    async def classify(self, post: SocialPost) -> SentimentResult:
        """Classify a single post."""
        ...

    async def classify_batch(self, posts: list[SocialPost]) -> list[SentimentResult]:
        """Classify a batch of posts (enables GPU batching)."""
        ...


# ─────────────────────────────────────────────
#  3. MARKET DATA PROVIDER — market_data layer
# ─────────────────────────────────────────────

@runtime_checkable
class MarketDataProvider(Protocol):
    """
    Abstracts live and historical price/volume data.
    Implementations: IBKRMarketData, YFinanceMarketData.
    """

    async def get_quote(self, ticker: str) -> dict:
        """Return last price, bid, ask, volume."""
        ...

    async def get_ohlcv(
        self, ticker: str, period: str = "1d", interval: str = "5m"
    ) -> list[dict]:
        """Return OHLCV bars."""
        ...

    async def get_vix(self) -> float:
        """Return current VIX level."""
        ...

    async def health_check(self) -> bool: ...


# ─────────────────────────────────────────────
#  4. EXECUTION ENGINE — execution layer
# ─────────────────────────────────────────────

@runtime_checkable
class ExecutionEngine(Protocol):
    """
    Abstracts order submission and position management.
    Implementations: PaperTradingEngine, IBKRExecutionEngine.
    """

    async def submit_signal(
        self, signal: Signal, quantity: int, stop_loss: float, take_profit: float
    ) -> OrderResult:
        """Submit a bracket order for a signal."""
        ...

    async def close_position(self, ticker: str, reason: str) -> OrderResult:
        """Market-close an open position."""
        ...

    async def get_positions(self) -> list[Position]:
        """Return all currently open positions."""
        ...

    async def get_account_state(self) -> AccountState:
        """Return current account equity and P&L."""
        ...

    async def health_check(self) -> bool: ...


# ─────────────────────────────────────────────
#  5. EVENT BUS — storage layer
# ─────────────────────────────────────────────

@runtime_checkable
class EventBus(Protocol):
    """
    Publish/subscribe on named streams.
    Implementation: TradingEventBus (Redis Streams).
    """

    async def publish(self, stream: str, event: dict) -> str:
        """Append event to stream, return message ID."""
        ...

    async def consume(
        self, stream: str, group: str, consumer: str, count: int = 10
    ) -> list[dict]:
        """Read unprocessed events from a consumer group."""
        ...

    async def ack(self, stream: str, group: str, message_id: str) -> None:
        """Acknowledge processed event."""
        ...


# ─────────────────────────────────────────────
#  6. REPOSITORY — storage layer
# ─────────────────────────────────────────────

@runtime_checkable
class TradeRepository(Protocol):
    """Persistence for trades, signals, and sentiment."""

    async def save_signal(self, signal: Signal) -> None: ...
    async def save_trade(self, trade: dict) -> None: ...
    async def get_recent_trades(self, days: int = 30) -> list[dict]: ...
    async def save_run_snapshot(self, config_hash: str, metrics: dict) -> None: ...
```

---

## Abstract Base Classes (`ingest/base.py`)

For data sources specifically, an ABC provides shared utility methods that
all implementations can inherit, while the Protocol ensures type-safety at call sites.

```python
from abc import ABC, abstractmethod
from typing import AsyncIterator
from social_trading.core.models import SocialPost
from social_trading.core.protocols import DataSource
from social_trading.config.system_config import SystemConfig
import redis.asyncio as aioredis
import logging

logger = logging.getLogger(__name__)


class BaseDataSource(ABC):
    """
    Base class for all data sources.
    Subclasses implement stream() or poll(); base class handles:
      - error retry with exponential backoff
      - publishing to raw_social Redis Stream
      - rate-limit tracking
    """

    def __init__(self, redis: aioredis.Redis, cfg: SystemConfig):
        self.redis = redis
        self.cfg = cfg
        self._errors = 0

    @property
    @abstractmethod
    def name(self) -> str: ...

    @property
    def is_streaming(self) -> bool:
        return False

    @abstractmethod
    async def stream(self) -> AsyncIterator[SocialPost]: ...

    @abstractmethod
    async def poll(self, tickers: list[str]) -> list[SocialPost]: ...

    @abstractmethod
    async def get_trending(self) -> list[str]: ...

    async def health_check(self) -> bool:
        try:
            await self.poll([])
            return True
        except Exception:
            return False

    async def _publish(self, post: SocialPost) -> None:
        """Publish a normalised post to the raw_social stream."""
        await self.redis.xadd("raw_social", post.model_dump(mode="json"))

    async def _publish_batch(self, posts: list[SocialPost]) -> None:
        async with self.redis.pipeline() as pipe:
            for post in posts:
                pipe.xadd("raw_social", post.model_dump(mode="json"))
            await pipe.execute()
        logger.debug("%s published %d posts", self.name, len(posts))
```

---

## DataSource Registry (`ingest/registry.py`)

The registry is the **plugin system** for data sources.
To add a new source: implement `BaseDataSource`, then register it.

```python
from __future__ import annotations
from social_trading.core.protocols import DataSource
import logging

logger = logging.getLogger(__name__)


class DataSourceRegistry:
    """
    Central registry for pluggable data sources.

    Usage:
        registry = DataSourceRegistry()
        registry.register(TwitterDataSource(redis, cfg))
        registry.register(RedditDataSource(reddit_client, redis, cfg))

        # enable/disable at runtime (reflected from config)
        sources = registry.active_sources()
    """

    def __init__(self) -> None:
        self._sources: dict[str, DataSource] = {}

    def register(self, source: DataSource) -> None:
        assert isinstance(source, DataSource), f"{source} does not satisfy DataSource protocol"
        self._sources[source.name] = source
        logger.info("Registered data source: %s", source.name)

    def unregister(self, name: str) -> None:
        self._sources.pop(name, None)

    def get(self, name: str) -> DataSource | None:
        return self._sources.get(name)

    def active_sources(self) -> list[DataSource]:
        return list(self._sources.values())

    @property
    def names(self) -> list[str]:
        return list(self._sources.keys())
```

---

## Dependency Injection Pattern

Services receive dependencies via constructor injection.
Tests replace real implementations with fakes that satisfy the same protocols.

```python
# src/social_trading/services/nlp_service.py  (real)
class NLPService:
    def __init__(
        self,
        bus: EventBus,                      # TradingEventBus in prod
        classifier: SentimentClassifier,    # FinBERTClassifier in prod
        cfg: SystemConfig,
    ):
        self.bus = bus
        self.classifier = classifier
        self.cfg = cfg
```

```python
# tests/unit/nlp/test_pipeline.py  (test uses fakes)
class FakeEventBus:
    published: list[dict] = []
    async def publish(self, stream, event):
        self.published.append({"stream": stream, **event})
        return "fake-id"
    async def consume(self, *args, **kwargs): return []
    async def ack(self, *args, **kwargs): return None

class FakeClassifier:
    model_name = "fake"
    async def classify(self, post):
        return SentimentResult(post_id=post.id, ticker=post.ticker,
                               positive=0.8, negative=0.1, neutral=0.1,
                               score=0.7, model="fake", latency_ms=0.1)
    async def classify_batch(self, posts):
        return [await self.classify(p) for p in posts]

def test_nlp_service_publishes_result():
    bus = FakeEventBus()
    svc = NLPService(bus=bus, classifier=FakeClassifier(), cfg=default_config())
    asyncio.run(svc.process_post(sample_post()))
    assert len(bus.published) == 1
    assert bus.published[0]["stream"] == "sentiment_signals"
```

---

## Stream Names (Event Contracts)

| Stream | Producer | Consumer | Payload |
|--------|----------|----------|---------|
| `raw_social` | ingest_service | nlp_service | `SocialPost` fields |
| `sentiment_signals` | nlp_service | signal_service | `SentimentResult` fields |
| `market_data` | market_data tasks | signal_service, risk_service | OHLCV fields |
| `strategy_signals` | signal_service | risk_service | `Signal` fields |
| `selected_signals` | risk_service | execution_service | `Signal` + position size |

---

*[⬆ Back to plan index](README.md)*
