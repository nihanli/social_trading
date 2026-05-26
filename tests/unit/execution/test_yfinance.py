"""Unit tests for YFinanceMarketData — all yfinance calls are mocked."""
from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from social_trading.market_data.yfinance import YFinanceMarketData

# ── Mock factories ─────────────────────────────────────────────────────────────

def make_ohlcv_df(n: int = 20, close_start: float = 100.0, step: float = 1.0) -> pd.DataFrame:
    """Build a minimal OHLCV DataFrame that yfinance would return."""

    index = pd.date_range("2024-01-01", periods=n, freq="D")
    closes = [close_start + i * step for i in range(n)]
    highs = [c * 1.01 for c in closes]
    lows = [c * 0.99 for c in closes]
    opens = [c * 1.005 for c in closes]
    volumes = [1_000_000.0] * n

    return pd.DataFrame(
        {"Open": opens, "High": highs, "Low": lows, "Close": closes, "Volume": volumes},
        index=index,
    )


def make_ticker_mock(last: float = 150.0, bid: float = 149.9, ask: float = 150.1) -> MagicMock:
    t = MagicMock()
    t.info = {
        "currentPrice": last,
        "bid": bid,
        "ask": ask,
        "regularMarketVolume": 5_000_000,
        "averageVolume": 3_000_000,
        "marketCap": 2_500_000_000_000,
    }
    t.fast_info = {}
    return t


# ── get_quote ─────────────────────────────────────────────────────────────────

async def test_get_quote_returns_expected_fields() -> None:
    ticker_mock = make_ticker_mock(last=178.50, bid=178.40, ask=178.60)
    provider = YFinanceMarketData(ticker_fn=lambda s: ticker_mock)
    quote = await provider.get_quote("AAPL")
    assert quote["last"] == pytest.approx(178.50)
    assert quote["bid"] == pytest.approx(178.40)
    assert quote["ask"] == pytest.approx(178.60)
    assert quote["volume"] == 5_000_000
    assert quote["avg_volume_30d"] == 3_000_000


async def test_get_quote_bid_ask_fallback() -> None:
    """When bid/ask are 0, fall back to last ± 0.1%."""
    ticker_mock = make_ticker_mock(last=100.0, bid=0.0, ask=0.0)
    provider = YFinanceMarketData(ticker_fn=lambda s: ticker_mock)
    quote = await provider.get_quote("AAPL")
    assert quote["bid"] == pytest.approx(99.9)
    assert quote["ask"] == pytest.approx(100.1)


# ── get_ohlcv ─────────────────────────────────────────────────────────────────

async def test_get_ohlcv_returns_bars() -> None:
    df = make_ohlcv_df(10)
    provider = YFinanceMarketData(download_fn=lambda *a, **kw: df)
    bars = await provider.get_ohlcv("AAPL", period="5d", interval="1d")
    assert len(bars) == 10
    assert "timestamp" in bars[0]
    assert "open" in bars[0]
    assert "close" in bars[0]
    assert "volume" in bars[0]


async def test_get_ohlcv_empty_df_returns_empty() -> None:
    provider = YFinanceMarketData(download_fn=lambda *a, **kw: pd.DataFrame())
    bars = await provider.get_ohlcv("AAPL")
    assert bars == []


async def test_get_ohlcv_ascending_order() -> None:
    df = make_ohlcv_df(5)
    provider = YFinanceMarketData(download_fn=lambda *a, **kw: df)
    bars = await provider.get_ohlcv("AAPL")
    closes = [b["close"] for b in bars]
    assert closes == sorted(closes)


# ── get_atr ───────────────────────────────────────────────────────────────────

async def test_get_atr_returns_positive_value() -> None:
    df = make_ohlcv_df(30, close_start=100.0)
    provider = YFinanceMarketData(download_fn=lambda *a, **kw: df)
    atr = await provider.get_atr("AAPL", period=14)
    assert atr > 0.0


async def test_get_atr_insufficient_data_returns_zero() -> None:
    df = make_ohlcv_df(5)  # < 14 period
    provider = YFinanceMarketData(download_fn=lambda *a, **kw: df)
    atr = await provider.get_atr("AAPL", period=14)
    assert atr == 0.0


async def test_get_atr_empty_returns_zero() -> None:
    provider = YFinanceMarketData(download_fn=lambda *a, **kw: pd.DataFrame())
    atr = await provider.get_atr("AAPL")
    assert atr == 0.0


async def test_get_atr_wilder_smoothing() -> None:
    """ATR should be smaller than the average high-low range for low-vol data."""
    df = make_ohlcv_df(50, close_start=100.0, step=0.1)  # very smooth trend
    provider = YFinanceMarketData(download_fn=lambda *a, **kw: df)
    atr = await provider.get_atr("AAPL", period=14)
    # High=close*1.01, Low=close*0.99 → TR ≈ 2% of price ≈ 2.0 for $100 stock
    assert 0.5 < atr < 5.0


# ── get_vix ───────────────────────────────────────────────────────────────────

async def test_get_vix_returns_value() -> None:
    vix_mock = MagicMock()
    vix_mock.info = {"regularMarketPrice": 18.5}
    provider = YFinanceMarketData(ticker_fn=lambda s: vix_mock)
    vix = await provider.get_vix()
    assert vix == pytest.approx(18.5)


async def test_get_vix_fallback_on_exception() -> None:
    def bad_ticker(s: str) -> None:
        raise RuntimeError("Network error")

    provider = YFinanceMarketData(ticker_fn=bad_ticker)
    vix = await provider.get_vix()
    assert vix == 20.0  # safe default


# ── get_realised_vol ──────────────────────────────────────────────────────────

async def test_get_realised_vol_returns_annualised() -> None:
    # Flat prices → near-zero vol
    df = make_ohlcv_df(40, close_start=100.0, step=0.0)
    provider = YFinanceMarketData(download_fn=lambda *a, **kw: df)
    vol = await provider.get_realised_vol("AAPL")
    assert vol >= 0.0


async def test_get_realised_vol_trending_up() -> None:
    df = make_ohlcv_df(40, close_start=100.0, step=1.0)  # 1% daily steps
    provider = YFinanceMarketData(download_fn=lambda *a, **kw: df)
    vol = await provider.get_realised_vol("AAPL")
    assert 0.0 <= vol <= 5.0  # annualised; consistent small-step trend


async def test_get_realised_vol_insufficient_data_returns_default() -> None:
    df = make_ohlcv_df(3)
    provider = YFinanceMarketData(download_fn=lambda *a, **kw: df)
    vol = await provider.get_realised_vol("AAPL")
    assert vol == 0.20


# ── health_check ──────────────────────────────────────────────────────────────

async def test_health_check_passes_when_quote_available() -> None:
    spy_mock = make_ticker_mock(last=450.0)
    provider = YFinanceMarketData(ticker_fn=lambda s: spy_mock)
    healthy = await provider.health_check()
    assert healthy is True


async def test_health_check_fails_when_last_zero() -> None:
    spy_mock = make_ticker_mock(last=0.0, bid=0.0, ask=0.0)
    spy_mock.info = {"currentPrice": 0.0, "bid": 0.0, "ask": 0.0}
    provider = YFinanceMarketData(ticker_fn=lambda s: spy_mock)
    healthy = await provider.health_check()
    assert healthy is False
