"""
company_info.py — cached company metadata lookup for Streamlit pages.

Uses yfinance for data.  Results are cached in Streamlit's cache for 24 hours
so repeated page refreshes do not hammer the yfinance API.

Typical usage:
    from social_trading.monitoring.streamlit.utils.company_info import (
        get_company_info, company_tooltip, enrich_df_with_company,
    )

    info = get_company_info("AAPL")
    # {'name': 'Apple Inc.', 'sector': 'Technology',
    #  'summary': 'Apple designs...', 'short_summary': 'Apple designs...'}

    # For Plotly hovertemplate custom data:
    names = [company_tooltip(t) for t in tickers]
"""
from __future__ import annotations

import logging

import streamlit as st

logger = logging.getLogger(__name__)

_UNKNOWN = {
    "name": "", "sector": "", "summary": "", "short_summary": "",
    # fundamentals
    "market_cap": None, "trailing_pe": None, "forward_pe": None,
    "price_to_book": None, "ev_to_ebitda": None,
    "dividend_yield": None, "beta": None,
    "fifty_two_week_high": None, "fifty_two_week_low": None,
    "average_volume": None, "profit_margin": None,
    "revenue_growth": None, "earnings_growth": None,
    "analyst_target": None, "industry": None,
    "employees": None, "exchange": None,
}


@st.cache_data(ttl=86_400, show_spinner=False)
def get_company_info(ticker: str) -> dict:
    """
    Return company metadata and key financial metrics for *ticker*.

    Metadata keys:
        name, sector, industry, exchange, employees
        summary, short_summary

    Fundamentals keys (None when unavailable):
        market_cap, trailing_pe, forward_pe, price_to_book, ev_to_ebitda,
        dividend_yield, beta,
        fifty_two_week_high, fifty_two_week_low, average_volume,
        profit_margin, revenue_growth, earnings_growth, analyst_target

    Returns a dict with empty strings / None if yfinance cannot be reached or
    the ticker is unknown, so callers never need to guard against missing keys.
    """
    try:
        import yfinance as yf  # noqa: PLC0415
        info = yf.Ticker(ticker).info
        name    = info.get("longName") or info.get("shortName") or ticker
        sector  = info.get("sector") or ""
        summary = info.get("longBusinessSummary") or ""
        short   = _first_sentences(summary, max_chars=220)

        def _f(key: str) -> float | None:
            v = info.get(key)
            try:
                return float(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        def _i(key: str) -> int | None:
            v = info.get(key)
            try:
                return int(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        return {
            "name":          name,
            "sector":        sector,
            "industry":      info.get("industry") or "",
            "exchange":      info.get("exchange") or "",
            "employees":     _i("fullTimeEmployees"),
            "summary":       summary,
            "short_summary": short,
            # fundamentals
            "market_cap":           _i("marketCap"),
            "trailing_pe":          _f("trailingPE"),
            "forward_pe":           _f("forwardPE"),
            "price_to_book":        _f("priceToBook"),
            "ev_to_ebitda":         _f("enterpriseToEbitda"),
            "dividend_yield":       _f("dividendYield"),
            "beta":                 _f("beta"),
            "fifty_two_week_high":  _f("fiftyTwoWeekHigh"),
            "fifty_two_week_low":   _f("fiftyTwoWeekLow"),
            "average_volume":       _i("averageVolume"),
            "profit_margin":        _f("profitMargins"),
            "revenue_growth":       _f("revenueGrowth"),
            "earnings_growth":      _f("earningsGrowth"),
            "analyst_target":       _f("targetMeanPrice"),
        }
    except Exception as exc:
        logger.debug("company_info(%s) failed: %s", ticker, exc)
        return {**_UNKNOWN}


def company_tooltip(ticker: str) -> str:
    """
    Return a compact one-line tooltip string for use in Plotly hovertemplate
    custom data, e.g. "Apple Inc. · Technology".
    """
    info = get_company_info(ticker)
    parts = [p for p in (info["name"], info["sector"]) if p]
    return " · ".join(parts) if parts else ticker


def enrich_tickers(tickers: list[str]) -> dict[str, str]:
    """
    Fetch company_tooltip for each ticker in *tickers*.
    Returns a dict mapping ticker → tooltip string.
    Suitable for vectorised use without N separate Streamlit cache calls.
    """
    return {t: company_tooltip(t) for t in tickers}


def fmt_market_cap(v: int | None) -> str:
    """Format a raw market cap integer as a human-readable string, e.g. '$1.23T'."""
    if v is None:
        return "—"
    if v >= 1_000_000_000_000:
        return f"${v / 1_000_000_000_000:.2f}T"
    if v >= 1_000_000_000:
        return f"${v / 1_000_000_000:.2f}B"
    if v >= 1_000_000:
        return f"${v / 1_000_000:.1f}M"
    return f"${v:,}"


def fmt_volume(v: int | None) -> str:
    """Format average daily volume, e.g. '24.5M'."""
    if v is None:
        return "—"
    if v >= 1_000_000:
        return f"{v / 1_000_000:.1f}M"
    if v >= 1_000:
        return f"{v / 1_000:.0f}K"
    return str(v)


def fmt_pct(v: float | None) -> str:
    """Format a 0-1 float as a percentage string, e.g. '12.3%'."""
    return f"{v * 100:.1f}%" if v is not None else "—"


def fmt_float(v: float | None, decimals: int = 2) -> str:
    """Format a float to given decimal places, or '—' when None."""
    return f"{v:.{decimals}f}" if v is not None else "—"


# ── Internal helpers ──────────────────────────────────────────────────────────

def _first_sentences(text: str, max_chars: int = 220) -> str:
    """Return the first two complete sentences of *text*, capped at max_chars."""
    if not text:
        return ""
    sentences: list[str] = []
    buf = ""
    for char in text:
        buf += char
        if char == "." and len(buf) > 10:
            sentences.append(buf.strip())
            buf = ""
            if len(" ".join(sentences)) >= max_chars or len(sentences) >= 2:
                break
    result = " ".join(sentences)
    if len(result) > max_chars:
        result = result[:max_chars].rsplit(" ", 1)[0] + "…"
    return result
