"""
Page 7 — Ticker Chart.

Candlestick OHLCV chart with:
  - IB primary / yfinance fallback data source
  - Timeframes: 5D (hourly), 1M / 3M / 6M (daily)
  - Signal buy/sell markers overlaid from the DB
  - Social mention volume bar overlay (from sentiment_aggregates)
  - Launched via URL query param: /chart?ticker=AAPL&tf=6M
  - Opens in a new browser tab from any page via chart_link helper
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../../../"))

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

from social_trading.monitoring.streamlit.utils.db import query, localize_datetimes
from social_trading.monitoring.streamlit.utils.chart_data import (
    fetch_ohlcv,
    TIMEFRAMES,
)
from social_trading.monitoring.streamlit.utils.company_info import (
    get_company_info,
    fmt_market_cap,
    fmt_volume,
    fmt_pct,
    fmt_float,
)

st.set_page_config(
    page_title="Ticker Chart",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Read URL params ────────────────────────────────────────────────────────────
params = st.query_params
url_ticker = params.get("ticker", "").upper().strip()
url_tf     = params.get("tf", "6M")
if url_tf not in TIMEFRAMES:
    url_tf = "6M"

# ── Controls row ───────────────────────────────────────────────────────────────
ctrl_col1, ctrl_col2, ctrl_col3 = st.columns([2, 3, 2])

with ctrl_col1:
    ticker = st.text_input(
        "Ticker",
        value=url_ticker,
        placeholder="e.g. AAPL",
        label_visibility="collapsed",
    ).upper().strip()

with ctrl_col2:
    tf_labels = {k: v["label"] for k, v in TIMEFRAMES.items()}
    tf_keys   = list(TIMEFRAMES.keys())
    tf_index  = tf_keys.index(url_tf) if url_tf in tf_keys else 1
    selected_label = st.radio(
        "Timeframe",
        options=list(tf_labels.values()),
        index=tf_index,
        horizontal=True,
        label_visibility="collapsed",
    )
    timeframe = tf_keys[list(tf_labels.values()).index(selected_label)]

with ctrl_col3:
    show_signals  = st.checkbox("Show signals",  value=True)
    show_mentions = st.checkbox("Show mentions", value=True)

# Sync query params to current selection (allows bookmarking)
if ticker:
    st.query_params["ticker"] = ticker
    st.query_params["tf"]     = timeframe

# ── Company info header ────────────────────────────────────────────────────────
_cinfo = get_company_info(ticker)
if _cinfo["name"]:
    _label_parts = [f"**{_cinfo['name']}**"]
    if _cinfo["sector"]:
        _label_parts.append(f"_{_cinfo['sector']}_")
    st.markdown("  ·  ".join(_label_parts))
    if _cinfo["short_summary"]:
        st.caption(_cinfo["short_summary"])

# ── Fetch OHLCV ────────────────────────────────────────────────────────────────
if not ticker:
    st.info("Enter a ticker symbol above to load the chart.")
    st.stop()

with st.spinner(f"Loading {ticker} ({TIMEFRAMES[timeframe]['label']})…"):
    try:
        df, source = fetch_ohlcv(ticker, timeframe)
    except Exception as exc:
        st.error(f"Could not load data for **{ticker}**: {exc}")
        st.stop()

if df.empty:
    st.warning(f"No data returned for **{ticker}** ({timeframe}).")
    st.stop()

# ── Fetch DB overlays ──────────────────────────────────────────────────────────
start_ts = df.index[0]
start_iso = start_ts.isoformat()

signals_df = pd.DataFrame()
if show_signals:
    signals_df = query(f"""
        SELECT direction,
               generated_at,
               ROUND(confidence::numeric, 3) AS quality,
               COALESCE(signal_phase, 'legacy') AS phase
        FROM signals
        WHERE ticker = '{ticker}'
          AND generated_at >= '{start_iso}'
        ORDER BY generated_at
    """)
    if not signals_df.empty:
        localize_datetimes(signals_df)

mentions_df = pd.DataFrame()
if show_mentions:
    tf_cfg = TIMEFRAMES[timeframe]
    # Bucket size: match the bar interval
    bucket_min = 60 if tf_cfg["interval"] == "1h" else 1440  # 1h or 1d
    mentions_df = query(f"""
        SELECT DATE_TRUNC('{"hour" if tf_cfg["interval"] == "1h" else "day"}',
                          window_start) AS bucket,
               SUM(post_count) AS mentions
        FROM sentiment_aggregates
        WHERE ticker = '{ticker}'
          AND window_start >= '{start_iso}'
        GROUP BY 1
        ORDER BY 1
    """)
    if not mentions_df.empty:
        localize_datetimes(mentions_df)

# ── Build chart ────────────────────────────────────────────────────────────────
n_rows   = 2 + (1 if show_mentions and not mentions_df.empty else 0)
row_heights = [0.60, 0.20] + ([0.20] if n_rows == 3 else [])

fig = make_subplots(
    rows=n_rows,
    cols=1,
    shared_xaxes=True,
    vertical_spacing=0.03,
    row_heights=row_heights,
    subplot_titles=[
        f"{ticker}  —  {TIMEFRAMES[timeframe]['label']}  (source: {source})",
        "Volume",
    ] + (["Social Mentions"] if n_rows == 3 else []),
)

# ── 1. Candlestick ─────────────────────────────────────────────────────────────
fig.add_trace(
    go.Candlestick(
        x=df.index,
        open=df["Open"],
        high=df["High"],
        low=df["Low"],
        close=df["Close"],
        name="Price",
        increasing_line_color="#26A69A",
        decreasing_line_color="#EF5350",
    ),
    row=1, col=1,
)

# ── 2. Signal markers ──────────────────────────────────────────────────────────
if show_signals and not signals_df.empty:
    # Ensure signal timestamps are tz-aware and comparable to df.index (NY tz)
    sig_ts = pd.to_datetime(signals_df["generated_at"], utc=True).dt.tz_convert(
        df.index.tz
    )

    for direction, color, symbol, label in [
        ("LONG",  "#2ECC71", "triangle-up",   "BUY"),
        ("SHORT", "#E74C3C", "triangle-down",  "SELL"),
    ]:
        mask = signals_df["direction"] == direction
        if mask.any():
            sub_ts     = sig_ts[mask]
            sub_meta   = signals_df[mask]
            # Snap each signal to the nearest bar's timestamp so it aligns
            # exactly on the x-axis with the candlestick bar.
            bar_x      = []
            sig_prices = []
            for ts in sub_ts:
                nearest_idx = (df.index - ts).to_series().abs().argmin()
                bar = df.iloc[nearest_idx]
                bar_x.append(df.index[nearest_idx])
                offset = (
                    bar["Low"] * 0.995
                    if direction == "LONG"
                    else bar["High"] * 1.005
                )
                sig_prices.append(offset)

            fig.add_trace(
                go.Scatter(
                    x=bar_x,
                    y=sig_prices,
                    mode="markers",
                    marker={
                        "symbol": symbol,
                        "size": 14,
                        "color": color,
                        "line": {"width": 1, "color": "white"},
                    },
                    name=label,
                    customdata=sub_meta[["quality", "phase"]].values,
                    hovertemplate=(
                        f"<b>{label}</b><br>"
                        "Quality: %{customdata[0]}<br>"
                        "Phase: %{customdata[1]}<br>"
                        "%{x}<extra></extra>"
                    ),
                ),
                row=1, col=1,
            )

# ── 3. Volume bars ─────────────────────────────────────────────────────────────
colors = [
    "#26A69A" if c >= o else "#EF5350"
    for o, c in zip(df["Open"], df["Close"])
]
fig.add_trace(
    go.Bar(
        x=df.index,
        y=df["Volume"],
        marker_color=colors,
        name="Volume",
        showlegend=False,
    ),
    row=2, col=1,
)

# ── 4. Social mentions ─────────────────────────────────────────────────────────
if n_rows == 3 and not mentions_df.empty:
    fig.add_trace(
        go.Bar(
            x=mentions_df["bucket"],
            y=mentions_df["mentions"],
            marker_color="#9B59B6",
            name="Mentions",
            showlegend=False,
        ),
        row=3, col=1,
    )

# ── Layout ─────────────────────────────────────────────────────────────────────
fig.update_layout(
    height=620,
    margin={"t": 40, "b": 20, "l": 10, "r": 10},
    xaxis_rangeslider_visible=False,
    plot_bgcolor="#0E1117",
    paper_bgcolor="#0E1117",
    font={"color": "#FAFAFA"},
    legend={
        "orientation": "h",
        "yanchor": "bottom",
        "y": 1.01,
        "xanchor": "right",
        "x": 1,
    },
    hovermode="x unified",
)
fig.update_xaxes(gridcolor="#1E2130", zeroline=False)
fig.update_yaxes(gridcolor="#1E2130", zeroline=False)

st.plotly_chart(fig, width='stretch')

# ── Info bar ───────────────────────────────────────────────────────────────────
last_bar = df.iloc[-1]
first_bar = df.iloc[0]
pct_chg = (last_bar["Close"] - first_bar["Open"]) / first_bar["Open"] * 100
chg_color = "#26A69A" if pct_chg >= 0 else "#EF5350"

info_cols = st.columns(6)
info_cols[0].metric("Last Close",  f"${last_bar['Close']:.2f}")
info_cols[1].metric("Open",        f"${last_bar['Open']:.2f}")
info_cols[2].metric("High",        f"${df['High'].max():.2f}")
info_cols[3].metric("Low",         f"${df['Low'].min():.2f}")
info_cols[4].metric("Period Chg",  f"{pct_chg:+.2f}%")
info_cols[5].metric(
    "Signals in window",
    len(signals_df) if not signals_df.empty else 0,
    help="Number of signals generated for this ticker within the displayed time window",
)

# ── Recent signals table ───────────────────────────────────────────────────────
if show_signals and not signals_df.empty:
    with st.expander(f"Signals for {ticker} in window ({len(signals_df)} total)", expanded=False):
        st.dataframe(
            signals_df.rename(columns={"generated_at": "time"}),
            width='stretch',
            hide_index=True,
        )

# ── Company fundamentals ───────────────────────────────────────────────────────
_ci = get_company_info(ticker)

_has_fundamentals = any(
    _ci.get(k) is not None
    for k in ("market_cap", "trailing_pe", "forward_pe", "beta",
              "dividend_yield", "price_to_book", "ev_to_ebitda",
              "profit_margin", "revenue_growth", "analyst_target")
)

if _has_fundamentals:
    st.divider()

    # Header row with industry / exchange context
    _header_parts: list[str] = []
    if _ci.get("industry"):
        _header_parts.append(_ci["industry"])
    if _ci.get("exchange"):
        _header_parts.append(_ci["exchange"])
    if _ci.get("employees"):
        _header_parts.append(f"{_ci['employees']:,} employees")
    st.caption("  ·  ".join(_header_parts) if _header_parts else "Company fundamentals")

    # ── Row 1: valuation ──────────────────────────────────────────────────────
    _v1, _v2, _v3, _v4, _v5 = st.columns(5)
    _v1.metric("Market Cap",    fmt_market_cap(_ci.get("market_cap")))
    _v2.metric("P/E (TTM)",     fmt_float(_ci.get("trailing_pe"), 1))
    _v3.metric("P/E (Forward)", fmt_float(_ci.get("forward_pe"), 1))
    _v4.metric("P/B Ratio",     fmt_float(_ci.get("price_to_book"), 2))
    _v5.metric("EV/EBITDA",     fmt_float(_ci.get("ev_to_ebitda"), 1))

    # ── Row 2: performance & risk ─────────────────────────────────────────────
    _r1, _r2, _r3, _r4, _r5 = st.columns(5)
    _r1.metric("Beta",          fmt_float(_ci.get("beta"), 2))
    _r2.metric("Dividend Yield",fmt_pct(_ci.get("dividend_yield")))
    _r3.metric("Profit Margin", fmt_pct(_ci.get("profit_margin")))
    _r4.metric("Revenue Growth",fmt_pct(_ci.get("revenue_growth")))
    _r5.metric("EPS Growth",    fmt_pct(_ci.get("earnings_growth")))

    # ── Row 3: price context ──────────────────────────────────────────────────
    _p1, _p2, _p3, _p4, _p5 = st.columns(5)
    _p1.metric("52W High",      f"${_ci['fifty_two_week_high']:.2f}" if _ci.get("fifty_two_week_high") else "—")
    _p2.metric("52W Low",       f"${_ci['fifty_two_week_low']:.2f}"  if _ci.get("fifty_two_week_low")  else "—")
    _p3.metric("Avg Volume",    fmt_volume(_ci.get("average_volume")))
    _p4.metric("Analyst Target",f"${_ci['analyst_target']:.2f}"      if _ci.get("analyst_target")      else "—")
    # 52W position — where current price sits in the 52-week range
    _hi = _ci.get("fifty_two_week_high")
    _lo = _ci.get("fifty_two_week_low")
    _last = last_bar["Close"]
    if _hi and _lo and _hi > _lo:
        _pos_pct = (_last - _lo) / (_hi - _lo) * 100
        _p5.metric("52W Position", f"{_pos_pct:.0f}%",
                   help="Where the current price sits in the 52-week High/Low range (100% = at 52W high)")
    else:
        _p5.metric("52W Position", "—")

