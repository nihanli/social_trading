"""
Domain exceptions — one exception class per distinct failure condition.

Services catch specific exceptions rather than broad Exception to enable
targeted handling (log + skip vs halt vs alert).
"""
from __future__ import annotations

# ─────────────────────────────────────────────────────────────────────────────
#  Infrastructure
# ─────────────────────────────────────────────────────────────────────────────

class TradingSystemError(Exception):
    """Base class for all domain exceptions."""


class ConfigurationError(TradingSystemError):
    """SystemConfig validation failed or required env var is missing."""


class DataSourceError(TradingSystemError):
    """A data source failed to return data (API error, auth failure, etc.)."""


class RateLimitError(DataSourceError):
    """API rate limit hit. Caller should back off."""

    def __init__(self, source: str, retry_after_seconds: float = 60.0) -> None:
        self.source = source
        self.retry_after_seconds = retry_after_seconds
        super().__init__(f"{source} rate limited; retry after {retry_after_seconds}s")


class MarketDataError(TradingSystemError):
    """Market data provider failed to return a quote or OHLCV bars."""


# ─────────────────────────────────────────────────────────────────────────────
#  Risk / Trading
# ─────────────────────────────────────────────────────────────────────────────

class CircuitBreakerOpen(TradingSystemError):
    """
    A circuit breaker is active — no new trades allowed until condition clears.
    Attributes:
        breaker_type: "daily_halt" | "weekly_reduce" | "drawdown_halt" | "single_trade"
        reset_at:     when the breaker auto-resets (None = manual reset required)
    """

    def __init__(self, breaker_type: str, detail: str = "", reset_at: str | None = None) -> None:
        self.breaker_type = breaker_type
        self.reset_at = reset_at
        msg = f"Circuit breaker active: {breaker_type}"
        if detail:
            msg += f" — {detail}"
        if reset_at:
            msg += f" (resets at {reset_at})"
        super().__init__(msg)


class InsufficientLiquidity(TradingSystemError):
    """Ticker failed the liquidity gate (low ADV, wide spread, or low market cap)."""

    def __init__(self, ticker: str, reason: str) -> None:
        self.ticker = ticker
        super().__init__(f"{ticker} failed liquidity gate: {reason}")


class PositionLimitExceeded(TradingSystemError):
    """Adding this position would exceed a concentration limit."""


class SignalQualityTooLow(TradingSystemError):
    """Signal quality score is below the configured threshold."""

    def __init__(self, ticker: str, score: float, threshold: float) -> None:
        self.ticker = ticker
        self.score = score
        self.threshold = threshold
        super().__init__(
            f"{ticker} signal quality {score:.3f} < threshold {threshold:.3f}"
        )


class SignalExpired(TradingSystemError):
    """Signal is older than max_signal_age_hours and should be discarded."""


# ─────────────────────────────────────────────────────────────────────────────
#  Execution
# ─────────────────────────────────────────────────────────────────────────────

class OrderRejected(TradingSystemError):
    """Broker rejected the order."""

    def __init__(self, ticker: str, reason: str) -> None:
        self.ticker = ticker
        super().__init__(f"Order rejected for {ticker}: {reason}")


class BrokerConnectionError(TradingSystemError):
    """Cannot connect to or lost connection to the broker."""


class DuplicatePosition(TradingSystemError):
    """Tried to open a position in a ticker that already has an open position."""

    def __init__(self, ticker: str) -> None:
        self.ticker = ticker
        super().__init__(f"Position already open for {ticker}")
