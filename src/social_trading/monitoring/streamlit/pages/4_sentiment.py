"""
Page 4 — Sentiment Heatmap.

Shows:
  - Top tickers by mention volume (coloured by sentiment score)
  - Per-ticker sentiment score timeline
  - Bot filter drop rate
  - Source breakdown pie
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../../../"))

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from social_trading.monitoring.streamlit.utils.db import query
from social_trading.monitoring.streamlit.utils.refresh_countdown import (
    sidebar_refresh_countdown,
)

st.set_page_config(page_title="Sentiment Heatmap", page_icon="🔥", layout="wide")
st_autorefresh(interval=15_000, key="sentiment_refresh")
sidebar_refresh_countdown()
st.title("Sentiment Heatmap")

# ── Time window selector ──────────────────────────────────────────────────────
hours = st.slider("Look-back window (hours)", 1, 24, 4)

# ── Top tickers heatmap ───────────────────────────────────────────────────────
heatmap_df = query(f"""
    SELECT ticker,
           SUM(post_count)     AS mentions,
           AVG(weighted_score) AS avg_sentiment,
           AVG(mention_zscore) AS avg_vol_z
    FROM sentiment_aggregates
    WHERE window_start > NOW() - INTERVAL '{hours} hours'
      AND window_minutes = 15
    GROUP BY ticker
    ORDER BY mentions DESC
    LIMIT 25
""")

if not heatmap_df.empty:
    fig = go.Figure(go.Bar(
        x=heatmap_df["ticker"],
        y=heatmap_df["mentions"],
        marker={
            "color": heatmap_df["avg_sentiment"],
            "colorscale": "RdYlGn",
            "cmin": -1,
            "cmax": 1,
            "colorbar": {"title": "Sentiment", "thickness": 14},
        },
        text=heatmap_df["avg_vol_z"].round(1).astype(str) + "σ",
        textposition="outside",
        hovertemplate=(
            "<b>%{x}</b><br>"
            "Mentions: %{y}<br>"
            "Avg Sentiment: %{marker.color:.3f}<br>"
            "Vol Z-score: %{text}<extra></extra>"
        ),
    ))
    fig.update_layout(
        title=f"Mention volume (last {hours}h) — colour = sentiment, label = volume Z-score",
        height=380,
        margin={"t": 50, "b": 20, "l": 10, "r": 10},
        xaxis_title=None,
        yaxis_title="Mentions",
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info(f"No sentiment data in the last {hours} hour(s)")

st.divider()

# ── Per-ticker sentiment timeline ─────────────────────────────────────────────
st.subheader("Sentiment Over Time — Top Tickers")
top_tickers = heatmap_df["ticker"].tolist()[:8] if not heatmap_df.empty else []

ticker_select = st.multiselect(
    "Select tickers",
    options=top_tickers or [],
    default=top_tickers[:4] if top_tickers else [],
)

if ticker_select:
    ticker_list = "','".join(ticker_select)
    timeline = query(f"""
        SELECT window_start AS time, ticker,
               ROUND(weighted_score::numeric, 4) AS sentiment,
               post_count
        FROM sentiment_aggregates
        WHERE window_start > NOW() - INTERVAL '{hours} hours'
          AND window_minutes = 15
          AND ticker IN ('{ticker_list}')
        ORDER BY time
    """)
    if not timeline.empty:
        fig2 = px.line(
            timeline,
            x="time",
            y="sentiment",
            color="ticker",
            title="Sentiment Score Timeline",
            labels={"sentiment": "Score", "time": ""},
        )
        fig2.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
        fig2.update_layout(height=300, margin={"t": 40, "b": 20})
        st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ── Source breakdown ──────────────────────────────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Posts by Source")
    source_df = query(f"""
        SELECT social_raw.source, COUNT(*) AS posts
        FROM sentiment_scores
        JOIN social_raw ON sentiment_scores.post_id = social_raw.post_id
        WHERE sentiment_scores.scored_at > NOW() - INTERVAL '{hours} hours'
        GROUP BY social_raw.source
        ORDER BY posts DESC
    """)
    if not source_df.empty:
        fig3 = px.pie(
            source_df,
            names="source",
            values="posts",
            title=f"Post sources (last {hours}h)",
        )
        fig3.update_layout(height=280)
        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info("No classified posts in window")

with col_right:
    st.subheader("Sentiment Label Distribution")
    label_df = query(f"""
        SELECT
          CASE
            WHEN (pos_prob - neg_prob) > 0.1  THEN 'Positive'
            WHEN (pos_prob - neg_prob) < -0.1 THEN 'Negative'
            ELSE 'Neutral'
          END AS label,
          COUNT(*) AS count
        FROM sentiment_scores
        WHERE scored_at > NOW() - INTERVAL '{hours} hours'
        GROUP BY 1
        ORDER BY count DESC
    """)
    if not label_df.empty:
        fig4 = px.pie(
            label_df,
            names="label",
            values="count",
            color="label",
            color_discrete_map={
                "Positive": "#28a745",
                "Neutral": "#6c757d",
                "Negative": "#dc3545",
            },
            title=f"Sentiment distribution (last {hours}h)",
        )
        fig4.update_layout(height=280)
        st.plotly_chart(fig4, use_container_width=True)
    else:
        st.info("No sentiment results in window")
