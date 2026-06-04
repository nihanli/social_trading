"""
Core domain models — shared across all services.
These are the canonical data shapes for the trading system.
No business logic here; logic lives in the service layers.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

# ─────────────────────────────────────────────────────────────────────────────
#  Social Data
# ─────────────────────────────────────────────────────────────────────────────

SourceName = Literal["twitter", "reddit", "stocktwits", "lunarcrush", "yfinance", "alpha_vantage", "ibkr", "bluesky"]


class SocialPost(BaseModel):
    """Normalised social media post from any data source."""

    id: str
    source: SourceName
    ticker: str
    text: str
    author_id: str
    author_followers: int = 0
    author_account_age_days: int = 0
    author_following: int = 0
    post_count_30d: int = 0          # posts by this author in last 30 days
    likes: int = 0
    reposts: int = 0
    is_original: bool = True         # False = repost/retweet
    url: str = ""
    collected_at: datetime = Field(default_factory=datetime.utcnow)
    raw: dict[str, Any] = Field(default_factory=dict)


# ─────────────────────────────────────────────────────────────────────────────
#  Sentiment
# ─────────────────────────────────────────────────────────────────────────────

class SentimentResult(BaseModel):
    """Output of any SentimentClassifier."""

    post_id: str
    ticker: str
    positive: float                  # probability [0, 1]
    negative: float
    neutral: float
    score: float                     # positive - negative  ∈ [-1, 1]
    model: str                       # "vader" | "finbert" | "stocktwits_native"
    latency_ms: float = 0.0
    classified_at: datetime = Field(default_factory=datetime.utcnow)
    # Engagement metadata forwarded from SocialPost for aggregation weighting
    source: str = ""                 # "twitter" | "reddit" | "stocktwits"
    likes: int = 0
    reposts: int = 0
    author_followers: int = 0


# ─────────────────────────────────────────────────────────────────────────────
#  Signals
# ─────────────────────────────────────────────────────────────────────────────

Direction = Literal["LONG", "SHORT"]


class Signal(BaseModel):
    """Strategy signal ready for risk review."""

    ticker: str
    direction: Direction
    quality_score: float             # 0–1 composite (design §5)
    sentiment_score: float           # ∈ [-1, 1]
    volume_z_score: float            # mentions vs 7-day baseline
    momentum: float                  # recent price change
    convergence: float               # fraction of sources agreeing [0, 1]
    proactivity: float = 1.0         # 1.0 = signal led price; 0.0 = reactive (price moved first)
    source_post_count: int
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: dict[str, Any] = Field(default_factory=dict)
    signal_phase: str | None = None  # "phase1" (free sources) or "phase2" (all sources)


# ─────────────────────────────────────────────────────────────────────────────
#  Execution
# ─────────────────────────────────────────────────────────────────────────────

OrderStatus = Literal["submitted", "filled", "rejected", "cancelled"]


class OrderResult(BaseModel):
    """Result returned from any ExecutionEngine."""

    order_id: str
    ticker: str
    direction: Direction
    quantity: int
    fill_price: float | None = None
    status: OrderStatus
    submitted_at: datetime = Field(default_factory=datetime.utcnow)
    error: str | None = None


@dataclass
class Position:
    """Currently open position. Field names match the DB positions table."""

    ticker: str
    direction: Direction
    shares: int               # DB: shares
    entry_price: float
    opened_at: datetime       # DB: opened_at
    stop_loss: float
    take_profit: float
    unrealized_pnl: float = 0.0   # DB: unrealized_pnl
    high_water_mark: float = 0.0  # for trailing stop
    signal_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def cost_basis(self) -> float:
        return self.entry_price * self.shares

    def update_hwm(self, current_price: float) -> None:
        if self.direction == "LONG":
            self.high_water_mark = max(self.high_water_mark, current_price)
        else:
            self.high_water_mark = min(
                self.high_water_mark if self.high_water_mark != 0.0 else current_price,
                current_price,
            )


@dataclass
class AccountState:
    """Snapshot of account equity used by risk components."""

    net_liquidation: float
    cash: float
    daily_pnl: float
    weekly_pnl: float
    drawdown_pct: float              # fraction below all-time peak (positive = drawdown)
    open_positions: list[Position] = field(default_factory=list)

    @property
    def social_exposure(self) -> float:
        """Total cost basis of all open positions as fraction of NLV."""
        if self.net_liquidation == 0:
            return 0.0
        total = sum(p.cost_basis for p in self.open_positions)
        return total / self.net_liquidation
