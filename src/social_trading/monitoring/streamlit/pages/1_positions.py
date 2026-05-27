"""
Page 1 — Open Positions detail.

Shows all currently open positions with:
  - P&L bar chart per ticker
  - Detailed table (entry, stop, target, hold time)
  - Per-position close button
  - Manual circuit breaker reset option
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../../../"))

import plotly.express as px
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from social_trading.monitoring.streamlit.utils.db import query
from social_trading.monitoring.streamlit.utils.redis_ctrl import (
    close_all_positions,
    close_position,
    get_system_state,
)

st.set_page_config(page_title="Positions", page_icon="📂", layout="wide")
st_autorefresh(interval=15_000, key="positions_refresh")
st.title("Open Positions")

state = get_system_state()

# ── Circuit breaker status banner ─────────────────────────────────────────────
if state["circuit"] != "NORMAL":
    st.error(f"Circuit breaker active: **{state['circuit']}** — new trades may be blocked")

# ── Positions table ───────────────────────────────────────────────────────────
positions = query("""
    SELECT
        ticker, direction, shares,
        ROUND(entry_price::numeric, 2)       AS entry_price,
        ROUND(stop_loss::numeric, 2)         AS stop_loss,
        ROUND(take_profit::numeric, 2)       AS take_profit,
        ROUND(unrealized_pnl::numeric, 2)    AS unrealized_pnl,
        ROUND((unrealized_pnl /
          NULLIF(entry_price * shares, 0) * 100)::numeric, 2) AS pnl_pct,
        ROUND(EXTRACT(EPOCH FROM (NOW() - opened_at)) / 3600, 1) AS hold_hrs,
        opened_at
    FROM positions
    ORDER BY unrealized_pnl DESC
""")

if positions.empty:
    st.info("No open positions")
    st.stop()

# KPI row
total_unrealized = positions["unrealized_pnl"].sum()
c1, c2, c3 = st.columns(3)
c1.metric("Open Positions", len(positions))
c2.metric("Total Unrealized P&L", f"${total_unrealized:+,.2f}")
c3.metric(
    "Longest Hold",
    f"{positions['hold_hrs'].max():.1f} hrs" if not positions.empty else "—",
)

# P&L bar chart
fig = px.bar(
    positions,
    x="ticker",
    y="unrealized_pnl",
    color="direction",
    color_discrete_map={"LONG": "#28a745", "SHORT": "#dc3545"},
    text="pnl_pct",
    title="Unrealized P&L by Position",
    labels={"unrealized_pnl": "P&L (USD)", "ticker": "Ticker"},
)
fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
fig.update_layout(height=300, margin={"t": 40, "b": 10})
st.plotly_chart(fig, use_container_width=True)

# Positions table
st.dataframe(
    positions.drop(columns=["opened_at"]),
    use_container_width=True,
    hide_index=True,
)

# ── Per-position close buttons ────────────────────────────────────────────────
st.subheader("Close Individual Positions")
cols = st.columns(min(len(positions), 6))
for i, (_, row) in enumerate(positions.iterrows()):
    with cols[i % 6]:
        pnl_label = f"${row['unrealized_pnl']:+.0f}"
        btn_type = "primary" if row["unrealized_pnl"] < 0 else "secondary"
        if st.button(
            f"Close {row['ticker']} ({pnl_label})",
            key=f"close_{row['ticker']}",
            type=btn_type,
            use_container_width=True,
        ):
            close_position(row["ticker"])
            st.warning(f"Close command sent for {row['ticker']}")

st.divider()

# ── Emergency close all ───────────────────────────────────────────────────────
with st.expander("Emergency Actions"):
    st.warning("These are immediate and irreversible.")
    if st.button("Close ALL Positions Now", type="primary"):
        close_all_positions()
        st.error("Close-all command published to execution engine")
