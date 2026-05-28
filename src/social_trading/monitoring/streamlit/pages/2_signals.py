"""
Page 2 — Signal Feed.

Shows:
  - Signal quality distribution (7 days)
  - Signal volume per hour (3 days) by direction
  - Full searchable/filterable signal table
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../../../"))

import plotly.express as px
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from social_trading.monitoring.streamlit.utils.db import query
from social_trading.monitoring.streamlit.utils.refresh_countdown import (
    sidebar_refresh_countdown,
)

st.set_page_config(page_title="Signal Feed", page_icon="⚡", layout="wide")
st_autorefresh(interval=15_000, key="signals_refresh")
sidebar_refresh_countdown()
st.title("Signal Feed")

# ── Quality distribution + volume timeline ────────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    quality_df = query("""
        SELECT ROUND(confidence::numeric, 1) AS quality_bucket,
               COUNT(*) AS count
        FROM signals
        WHERE generated_at > NOW() - INTERVAL '7 days'
        GROUP BY 1
        ORDER BY 1
    """)
    if not quality_df.empty:
        fig = px.bar(
            quality_df,
            x="quality_bucket",
            y="count",
            title="Signal Quality Distribution (7 days)",
            labels={"quality_bucket": "Quality Score", "count": "Count"},
        )
        fig.update_layout(height=300, margin={"t": 40, "b": 10})
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No signal history yet")

with col_right:
    signal_ts = query("""
        SELECT DATE_TRUNC('hour', generated_at) AS hour,
               direction, COUNT(*) AS count
        FROM signals
        WHERE generated_at > NOW() - INTERVAL '3 days'
        GROUP BY 1, 2
        ORDER BY 1
    """)
    if not signal_ts.empty:
        fig2 = px.bar(
            signal_ts,
            x="hour",
            y="count",
            color="direction",
            title="Signals per Hour (3 days)",
            color_discrete_map={"LONG": "#28a745", "SHORT": "#dc3545", "FLAT": "#6c757d"},
        )
        fig2.update_layout(height=300, margin={"t": 40, "b": 10})
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No signal history yet")

# ── Approval funnel stats ─────────────────────────────────────────────────────
funnel = query("""
    SELECT
        COUNT(*)                                    AS total,
        COUNT(*) FILTER (WHERE approved)            AS approved,
        COUNT(*) FILTER (WHERE executed)            AS executed,
        ROUND(AVG(confidence)::numeric, 3)          AS avg_quality
    FROM signals
    WHERE generated_at > NOW() - INTERVAL '7 days'
""")
if not funnel.empty and funnel.iloc[0]["total"] > 0:
    f = funnel.iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Signals (7d)", int(f["total"]))
    c2.metric("Approved", int(f["approved"]))
    c3.metric("Executed", int(f["executed"]))
    c4.metric("Avg Quality", f"{f['avg_quality']:.3f}")

st.divider()

# ── Full searchable signal table ──────────────────────────────────────────────
st.subheader("All Signals")
col1, col2, col3 = st.columns(3)
ticker_filter = col1.text_input("Filter ticker")
direction_filter = col2.selectbox("Direction", ["All", "LONG", "SHORT", "FLAT"])
days_filter = col3.selectbox("Time window", [1, 3, 7, 30], index=1)

where = f"WHERE generated_at > NOW() - INTERVAL '{days_filter} days'"
if ticker_filter:
    where += f" AND ticker ILIKE '%{ticker_filter}%'"
if direction_filter != "All":
    where += f" AND direction = '{direction_filter}'"

full_signals = query(f"""
    SELECT ticker, direction,
           ROUND(confidence::numeric, 3)      AS quality,
           ROUND(sentiment_score::numeric, 3) AS sentiment,
           ROUND(mention_zscore::numeric, 2)  AS vol_z,
           approved, executed,
           generated_at
    FROM signals
    {where}
    ORDER BY generated_at DESC
    LIMIT 300
""")

if not full_signals.empty:
    st.dataframe(full_signals, use_container_width=True, hide_index=True)
    st.caption(f"{len(full_signals)} signals shown (max 300)")
else:
    st.info("No signals match the current filters")
