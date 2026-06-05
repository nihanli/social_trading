"""
Page 3 — Trade Analytics.

Shows:
  - Performance stats (total trades, win rate, P&L, avg hold time)
  - Cumulative P&L curve
  - Daily P&L bar chart
  - P&L by exit reason
  - Full closed trade table
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../../../"))

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from social_trading.monitoring.streamlit.utils.db import query, localize_datetimes
from social_trading.monitoring.streamlit.utils.refresh_countdown import (
    sidebar_refresh_countdown,
)
from social_trading.monitoring.streamlit.utils.company_info import enrich_tickers
from social_trading.monitoring.streamlit.utils.table_html import render_table

st.set_page_config(page_title="Trade Analytics", page_icon="📊", layout="wide")
st_autorefresh(interval=15_000, key="trades_refresh")
sidebar_refresh_countdown()
st.title("Trade Analytics")

mode = st.radio("Trading Mode", ["paper", "live"], horizontal=True)

# ── Performance KPIs ──────────────────────────────────────────────────────────
stats = query(f"""
    SELECT
      COUNT(*)                                              AS total_trades,
      COUNT(*) FILTER (WHERE net_pnl > 0)                  AS wins,
      ROUND(AVG(net_pnl)::numeric, 2)                      AS avg_pnl,
      ROUND(MAX(net_pnl)::numeric, 2)                      AS best_trade,
      ROUND(MIN(net_pnl)::numeric, 2)                      AS worst_trade,
      ROUND(SUM(net_pnl)::numeric, 2)                      AS total_pnl,
      ROUND(AVG(EXTRACT(EPOCH FROM (closed_at - opened_at))
            / 3600)::numeric, 1)                           AS avg_hold_hrs
    FROM trades
    WHERE mode = '{mode}' AND closed_at IS NOT NULL
""")

if not stats.empty and stats.iloc[0]["total_trades"]:
    s = stats.iloc[0]
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Trades", int(s["total_trades"]))
    c2.metric("Win Rate", f"{100 * s['wins'] / max(s['total_trades'], 1):.1f}%")
    c3.metric("Total P&L", f"${s['total_pnl']:+,.2f}")
    c4.metric("Avg P&L / Trade", f"${s['avg_pnl']:+,.2f}")
    c5.metric("Avg Hold Time", f"{s['avg_hold_hrs']} hrs")

    extra_c1, extra_c2, extra_c3 = st.columns(3)
    extra_c1.metric("Best Trade", f"${s['best_trade']:+,.2f}")
    extra_c2.metric("Worst Trade", f"${s['worst_trade']:+,.2f}")
else:
    st.info(f"No closed {mode} trades yet")
    st.stop()

st.divider()

# ── Cumulative P&L curve ──────────────────────────────────────────────────────
cum_pnl = query(f"""
    SELECT closed_at AS time,
           SUM(net_pnl) OVER (ORDER BY closed_at) AS cumulative_pnl
    FROM trades
    WHERE mode = '{mode}' AND closed_at IS NOT NULL
    ORDER BY closed_at
""")
if not cum_pnl.empty:
    localize_datetimes(cum_pnl)
    last_val = cum_pnl["cumulative_pnl"].iloc[-1]
    fig = go.Figure(go.Scatter(
        x=cum_pnl["time"],
        y=cum_pnl["cumulative_pnl"],
        fill="tozeroy",
        line={"color": "#28a745" if last_val >= 0 else "#dc3545", "width": 2},
        name="Cumulative P&L",
    ))
    fig.update_layout(
        title="Cumulative P&L",
        height=280,
        margin={"t": 30, "b": 20},
        yaxis_title="USD",
    )
    st.plotly_chart(fig, width='stretch')

# ── Daily P&L bars ────────────────────────────────────────────────────────────
daily_pnl = query(f"""
    SELECT
        DATE_TRUNC('day', closed_at) AS day,
        ROUND(SUM(net_pnl)::numeric, 2) AS daily_pnl
    FROM trades
    WHERE mode = '{mode}' AND closed_at IS NOT NULL
    GROUP BY 1
    ORDER BY 1
""")
if not daily_pnl.empty:
    localize_datetimes(daily_pnl)
    fig2 = px.bar(
        daily_pnl,
        x="day",
        y="daily_pnl",
        color="daily_pnl",
        color_continuous_scale=["#dc3545", "#ffc107", "#28a745"],
        title="Daily P&L",
        labels={"daily_pnl": "P&L (USD)", "day": "Date"},
    )
    fig2.update_layout(height=280, margin={"t": 30, "b": 20}, coloraxis_showscale=False)
    st.plotly_chart(fig2, width='stretch')

st.divider()

# ── P&L by exit reason ────────────────────────────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    by_exit = query(f"""
        SELECT exit_reason,
               COUNT(*)                             AS trades,
               ROUND(SUM(net_pnl)::numeric, 2)      AS total_pnl,
               ROUND(AVG(net_pnl)::numeric, 2)      AS avg_pnl,
               COUNT(*) FILTER (WHERE net_pnl > 0)  AS wins
        FROM trades
        WHERE mode = '{mode}' AND closed_at IS NOT NULL
        GROUP BY exit_reason
        ORDER BY total_pnl DESC
    """)
    if not by_exit.empty:
        st.subheader("P&L by Exit Reason")
        st.dataframe(by_exit, width='stretch', hide_index=True)

with col_right:
    by_ticker = query(f"""
        SELECT ticker,
               COUNT(*) AS trades,
               ROUND(SUM(net_pnl)::numeric, 2) AS total_pnl,
               ROUND(AVG(net_pnl)::numeric, 2) AS avg_pnl
        FROM trades
        WHERE mode = '{mode}' AND closed_at IS NOT NULL
        GROUP BY ticker
        ORDER BY total_pnl DESC
        LIMIT 15
    """)
    if not by_ticker.empty:
        st.subheader("P&L by Ticker")
        st.dataframe(by_ticker, width='stretch', hide_index=True)

st.divider()

# ── Full trade table ──────────────────────────────────────────────────────────
st.subheader("All Closed Trades")
days = st.selectbox("Show last N days", [7, 30, 90, 365], index=1)
all_trades = query(f"""
    SELECT ticker,
           '/chart?ticker=' || ticker || '&tf=6M' AS chart,
           direction, shares,
           ROUND(entry_price::numeric, 2) AS entry,
           ROUND(exit_price::numeric, 2)  AS exit,
           ROUND(net_pnl::numeric, 2)     AS net_pnl,
           exit_reason,
           TO_CHAR(opened_at,  'MM-DD HH24:MI') AS opened,
           TO_CHAR(closed_at,  'MM-DD HH24:MI') AS closed
    FROM trades
    WHERE mode = '{mode}'
      AND closed_at IS NOT NULL
      AND closed_at > NOW() - INTERVAL '{days} days'
    ORDER BY closed_at DESC
    LIMIT 500
""")
if not all_trades.empty:
    _trade_names = enrich_tickers(all_trades["ticker"].unique().tolist())
    render_table(
        all_trades,
        tooltips={"ticker": _trade_names},
        link_cols={"chart": ("📈", "_blank")},
    )
    st.caption(f"{len(all_trades)} trades shown (max 500)")
else:
    st.info(f"No closed {mode} trades in the last {days} days")
