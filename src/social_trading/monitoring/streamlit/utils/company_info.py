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

_UNKNOWN = {"name": "", "sector": "", "summary": "", "short_summary": ""}


@st.cache_data(ttl=86_400, show_spinner=False)
def get_company_info(ticker: str) -> dict[str, str]:
    """
    Return company metadata for *ticker*.

    Keys:
        name          – full company name, e.g. "Apple Inc."
        sector        – sector string, e.g. "Technology"
        summary       – full longBusinessSummary (may be long)
        short_summary – first two sentences, ≤ 200 chars

    Returns a dict with empty strings if yfinance cannot be reached or the
    ticker is unknown, so callers never need to guard against missing keys.
    """
    try:
        import yfinance as yf  # noqa: PLC0415
        info = yf.Ticker(ticker).info
        name    = info.get("longName") or info.get("shortName") or ticker
        sector  = info.get("sector") or ""
        summary = info.get("longBusinessSummary") or ""
        short   = _first_sentences(summary, max_chars=220)
        return {
            "name":          name,
            "sector":        sector,
            "summary":       summary,
            "short_summary": short,
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
