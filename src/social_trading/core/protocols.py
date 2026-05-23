"""
Protocol interfaces — the contracts between all system components.

Every cross-service dependency must go through one of these protocols.
Concrete implementations live in their own sub-packages; nothing imports
them except the service wiring layer (services/) and tests.

Using typing.Protocol (structural subtyping): any class that implements
the required methods satisfies the protocol — no inheritance needed.
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any, Protocol, runtime_checkable

from .models import (
    AccountState,
    OrderResult,
    Position,
    SentimentResult,
    Signal,
    SocialPost,
)

# ─────────────────────────────────────────────────────────────────────────────
#  1. Data Source  (ingest layer)
# ─────────────────────────────────────────────────────────────────────────────

@runtime_checkable
class DataSource(Protocol):
    """
    Any social media data source must satisfy this protocol.
    Implementations: TwitterDataSource, RedditDataSource, StockTwitsDataSource.

    is_streaming=True  → service calls stream() and iterates indefinitely.
    is_streaming=False → service calls poll() on a timer.
    """

    name: str
    is_streaming: bool

    async def stream(self) -> AsyncIterator[SocialPost]:
        """Yield posts as they arrive (streaming sources only)."""
        ...

    async def poll(self, tickers: list[str]) -> list[SocialPost]:
        """Fetch recent posts for given tickers (polling sources)."""
        ...

    async def get_trending(self) -> list[str]:
        """Return currently trending tickers on this platform."""
        ...

    async def health_check(self) -> bool:
        """Return True if the API is reachable and credentials are valid."""
        ...


# ─────────────────────────────────────────────────────────────────────────────
#  2. Sentiment Classifier  (NLP layer)
# ─────────────────────────────────────────────────────────────────────────────

@runtime_checkable
class SentimentClassifier(Protocol):
    """
    Any sentiment model must satisfy this protocol.
    Implementations: VaderClassifier (fast pre-filter), FinBERTClassifier (primary).
    """

    model_name: str

    async def classify(self, post: SocialPost) -> SentimentResult:
        """Classify a single post."""
        ...

    async def classify_batch(self, posts: list[SocialPost]) -> list[SentimentResult]:
        """Classify a batch — enables GPU batching for FinBERT."""
        ...


# ─────────────────────────────────────────────────────────────────────────────
#  3. Market Data Provider  (market_data layer)
# ─────────────────────────────────────────────────────────────────────────────

@runtime_checkable
class MarketDataProvider(Protocol):
    """
    Abstracts live and historical price/volume data.
    Implementations: IBKRMarketData (live), YFinanceMarketData (backtest/watchlist).
    """

    async def get_quote(self, ticker: str) -> dict[str, Any]:
        """Return dict with keys: last, bid, ask, volume, avg_volume_30d."""
        ...

    async def get_ohlcv(
        self, ticker: str, period: str = "1d", interval: str = "5m"
    ) -> list[dict[str, Any]]:
        """Return list of OHLCV bar dicts sorted ascending by timestamp."""
        ...

    async def get_atr(self, ticker: str, period: int = 14) -> float:
        """Return ATR(period) in price units."""
        ...

    async def get_vix(self) -> float:
        """Return current VIX level."""
        ...

    async def health_check(self) -> bool: ...


# ─────────────────────────────────────────────────────────────────────────────
#  4. Execution Engine  (execution layer)
# ─────────────────────────────────────────────────────────────────────────────

@runtime_checkable
class ExecutionEngine(Protocol):
    """
    Abstracts order submission and position management.
    Implementations: PaperTradingEngine (tests/dev), IBKRExecutionEngine (live).
    """

    async def submit_signal(
        self,
        signal: Signal,
        quantity: int,
        stop_loss: float,
        take_profit: float,
    ) -> OrderResult:
        """Submit a bracket order (entry + stop + target) for a signal."""
        ...

    async def close_position(self, ticker: str, reason: str = "") -> OrderResult:
        """Market-close an open position."""
        ...

    async def get_positions(self) -> list[Position]:
        """Return all currently open positions."""
        ...

    async def get_account_state(self) -> AccountState:
        """Return current account equity, cash, and P&L snapshot."""
        ...

    async def health_check(self) -> bool: ...


# ─────────────────────────────────────────────────────────────────────────────
#  5. Event Bus  (storage layer)
# ─────────────────────────────────────────────────────────────────────────────

@runtime_checkable
class EventBus(Protocol):
    """
    Publish/subscribe on named streams.
    Implementation: TradingEventBus (Redis Streams).

    Stream names:
        raw_social       – ingest → nlp
        sentiment_signals – nlp → signal
        market_data       – market_data tasks → signal, risk
        strategy_signals  – signal → risk
        selected_signals  – risk → execution
    """

    async def publish(self, stream: str, event: dict[str, Any]) -> str:
        """Append event to stream. Returns message ID."""
        ...

    async def consume(
        self,
        stream: str,
        group: str,
        consumer: str,
        count: int = 10,
        block_ms: int = 2000,
    ) -> list[tuple[str, dict[str, Any]]]:
        """
        Read unprocessed events from a consumer group.
        Returns list of (message_id, fields) tuples.
        Blocks for block_ms if no messages available.
        """
        ...

    async def ack(self, stream: str, group: str, message_id: str) -> None:
        """Acknowledge a processed event so it won't be re-delivered."""
        ...

    async def create_group(self, stream: str, group: str) -> None:
        """Create a consumer group (idempotent)."""
        ...


# ─────────────────────────────────────────────────────────────────────────────
#  6. Trade Repository  (storage layer)
# ─────────────────────────────────────────────────────────────────────────────

@runtime_checkable
class TradeRepository(Protocol):
    """Persistence for trades, signals, and sentiment data."""

    async def save_signal(self, signal: Signal, approved: bool) -> int:
        """Persist a signal. Returns the database ID."""
        ...

    async def save_trade(self, trade: dict[str, Any]) -> int:
        """Persist an executed trade. Returns database ID."""
        ...

    async def close_trade(self, trade_id: int, exit_price: float, reason: str) -> None:
        """Mark a trade closed with exit price and reason."""
        ...

    async def get_recent_trades(self, days: int = 30) -> list[dict[str, Any]]:
        """Return trades from the last N days."""
        ...

    async def save_run_snapshot(self, config_hash: str, metrics: dict[str, Any]) -> None:
        """Save EOD config + performance snapshot for optimization feedback."""
        ...
