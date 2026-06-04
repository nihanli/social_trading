"""
chart_link.py — helpers for rendering ticker → chart page links in Streamlit.

Usage (in any page):
    from social_trading.monitoring.streamlit.utils.chart_link import (
        chart_link, ticker_links_html
    )

    # Single link (use with st.markdown unsafe_allow_html=True):
    st.markdown(chart_link("AAPL"), unsafe_allow_html=True)

    # Inject a "📈" link column into a DataFrame before display:
    df["chart"] = df["ticker"].apply(chart_link)
    st.markdown(ticker_links_html(df["ticker"].tolist()), unsafe_allow_html=True)
"""
from __future__ import annotations

_CHART_BASE = "/chart"


def chart_link(ticker: str, timeframe: str = "1M") -> str:
    """
    Return an HTML anchor that opens the chart page in a new browser tab.

    Args:
        ticker:    Ticker symbol, e.g. "AAPL"
        timeframe: Default timeframe to pre-select ("5D", "1M", "3M", "6M")

    Returns:
        HTML string: <a href="/chart?ticker=AAPL&tf=1M" target="_blank">📈 AAPL</a>
    """
    url = f"{_CHART_BASE}?ticker={ticker.upper()}&tf={timeframe}"
    return (
        f'<a href="{url}" target="_blank" '
        f'style="text-decoration:none;color:#4A90D9;font-weight:bold;">'
        f"📈 {ticker.upper()}</a>"
    )


def chart_icon_link(ticker: str, timeframe: str = "1M") -> str:
    """
    Return a compact icon-only link (no label) — useful in tight table columns.
    """
    url = f"{_CHART_BASE}?ticker={ticker.upper()}&tf={timeframe}"
    return (
        f'<a href="{url}" target="_blank" '
        f'title="Open {ticker.upper()} chart" '
        f'style="text-decoration:none;">📈</a>'
    )


def ticker_links_html(tickers: list[str], timeframe: str = "1M") -> str:
    """
    Return an HTML block with one link per ticker (space-separated).
    Suitable for rendering a row of ticker links via st.markdown.
    """
    links = [chart_link(t, timeframe) for t in tickers]
    return "  ".join(links)
