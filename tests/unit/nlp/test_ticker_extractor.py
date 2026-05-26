"""Unit tests for TickerExtractor."""
from __future__ import annotations

import pytest

from social_trading.nlp.filters.ticker_extractor import TickerExtractor


@pytest.fixture
def extractor() -> TickerExtractor:
    return TickerExtractor(use_spacy=False)


UNIVERSE: set[str] = {"AAPL", "TSLA", "AMD", "NVDA", "MSFT", "SPY", "QQQ"}


# ── Pass 1: cashtag regex ─────────────────────────────────────────────────────

def test_cashtag_basic(extractor: TickerExtractor) -> None:
    result = extractor.extract("$AAPL is up 5% today", UNIVERSE)
    assert "AAPL" in result


def test_cashtag_multiple(extractor: TickerExtractor) -> None:
    result = extractor.extract("Sold $TSLA, bought $AMD and $NVDA", UNIVERSE)
    assert set(result) == {"TSLA", "AMD", "NVDA"}


def test_cashtag_case_insensitive(extractor: TickerExtractor) -> None:
    result = extractor.extract("$aapl is great", UNIVERSE)
    assert "AAPL" in result


def test_cashtag_not_in_universe_excluded(extractor: TickerExtractor) -> None:
    result = extractor.extract("$AAPL and $FAKE are trending", UNIVERSE)
    assert "AAPL" in result
    assert "FAKE" not in result


def test_cashtag_no_valid_tickers_returns_all(extractor: TickerExtractor) -> None:
    """When valid_tickers is None, return all cashtags without validation."""
    result = extractor.extract("$AAPL $FAKE $XYZ")
    assert set(result) == {"AAPL", "FAKE", "XYZ"}


def test_cashtag_too_long_excluded(extractor: TickerExtractor) -> None:
    """Ticker with > 5 chars should not be matched."""
    result = extractor.extract("$TOOLONG is interesting", UNIVERSE)
    assert "TOOLONG" not in result


# ── Pass 3: standalone uppercase words ───────────────────────────────────────

def test_uppercase_word_in_universe(extractor: TickerExtractor) -> None:
    result = extractor.extract("I think AAPL will hit ATH", UNIVERSE)
    assert "AAPL" in result


def test_stop_word_excluded(extractor: TickerExtractor) -> None:
    """Common words like THE, AND, IS should never be returned."""
    result = extractor.extract("The SPY ETF is down today BUT AAPL is fine", UNIVERSE)
    assert "THE" not in result
    assert "AND" not in result
    assert "BUT" not in result
    assert "ETF" not in result
    assert "SPY" in result
    assert "AAPL" in result


def test_finance_jargon_excluded(extractor: TickerExtractor) -> None:
    """YOLO, MOON, HODL etc should not be returned."""
    result = extractor.extract("YOLO into TSLA going to the MOON", UNIVERSE)
    assert "YOLO" not in result
    assert "MOON" not in result
    assert "TSLA" in result


# ── Deduplication ─────────────────────────────────────────────────────────────

def test_deduplication(extractor: TickerExtractor) -> None:
    """Same ticker from multiple passes should appear once."""
    result = extractor.extract("$AAPL AAPL mentioned twice", UNIVERSE)
    assert result.count("AAPL") == 1


# ── Empty / edge cases ────────────────────────────────────────────────────────

def test_empty_text(extractor: TickerExtractor) -> None:
    assert extractor.extract("", UNIVERSE) == []


def test_no_tickers_in_text(extractor: TickerExtractor) -> None:
    assert extractor.extract("Just a regular sentence with no tickers", UNIVERSE) == []


def test_numeric_text(extractor: TickerExtractor) -> None:
    assert extractor.extract("123 456 789", UNIVERSE) == []


# ── Result is sorted ─────────────────────────────────────────────────────────

def test_result_is_sorted(extractor: TickerExtractor) -> None:
    result = extractor.extract("$TSLA $AAPL $NVDA rocks", UNIVERSE)
    assert result == sorted(result)
