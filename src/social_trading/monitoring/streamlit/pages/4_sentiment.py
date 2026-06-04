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

import redis as _redis_lib

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from social_trading.monitoring.streamlit.utils.db import query, localize_datetimes
from social_trading.monitoring.streamlit.utils.refresh_countdown import (
    sidebar_refresh_countdown,
)
from social_trading.monitoring.streamlit.utils.company_info import enrich_tickers

st.set_page_config(page_title="Sentiment Heatmap", page_icon="🔥", layout="wide")
st_autorefresh(interval=15_000, key="sentiment_refresh")
sidebar_refresh_countdown()
st.title("Sentiment Heatmap")

# ── Time window selector ──────────────────────────────────────────────────────
hours = st.slider("Look-back window (hours)", 1, 24, 4)

# ── Top tickers heatmap ───────────────────────────────────────────────────────
heatmap_df = query(f"""
    WITH agg AS (
        SELECT ticker,
               SUM(post_count)     AS mentions,
               AVG(weighted_score) AS avg_sentiment
        FROM sentiment_aggregates
        WHERE window_start > NOW() - INTERVAL '{hours} hours'
          AND window_minutes = 15
        GROUP BY ticker
    )
    SELECT ticker, mentions, avg_sentiment,
           (mentions - AVG(mentions) OVER ())
               / NULLIF(STDDEV_SAMP(mentions) OVER (), 0) AS avg_vol_z
    FROM agg
    ORDER BY mentions DESC
    LIMIT 25
""")

if not heatmap_df.empty:
    _tooltips = enrich_tickers(heatmap_df["ticker"].tolist())
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
        text=heatmap_df["avg_vol_z"].fillna(0).round(1).astype(str) + "σ",
        textposition="outside",
        customdata=[_tooltips.get(t, t) for t in heatmap_df["ticker"]],
        hovertemplate=(
            "<b>%{x}</b>  <i>%{customdata}</i><br>"
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
    st.plotly_chart(fig, width='stretch')
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
        localize_datetimes(timeline)
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
        st.plotly_chart(fig2, width='stretch')

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
        st.plotly_chart(fig3, width='stretch')
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
        st.plotly_chart(fig4, width='stretch')
    else:
        st.info("No sentiment results in window")

st.divider()

# ── Mentions by source × ticker ───────────────────────────────────────────────
st.subheader("Mention Count by Source")
st.caption(
    "**Text sources** (Bluesky, StockTwits…) are counted from the DB within the "
    "selected time window.  **Volume-only sources** (ApeWisdom) store rolling "
    "counts in Redis — the most recent 24 h count is shown for each ticker."
)

# ── DB text sources ────────────────────────────────────────────────────────────
src_ticker_df = query(f"""
    SELECT ticker, source, COUNT(*) AS mentions
    FROM social_raw
    WHERE ticker IS NOT NULL
      AND created_at > NOW() - INTERVAL '{hours} hours'
    GROUP BY ticker, source
    ORDER BY mentions DESC
""")

# ── ApeWisdom from Redis ───────────────────────────────────────────────────────
def _read_apewisdom_redis() -> list[dict]:
    rows = []
    try:
        url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        r = _redis_lib.from_url(url, decode_responses=True)
        for key in r.scan_iter("mention_history:apewisdom:*"):
            ticker = key.split(":")[-1]
            last = r.lindex(key, -1)          # most recent count
            if last is not None:
                rows.append({"ticker": ticker, "source": "apewisdom",
                             "mentions": int(float(last))})
    except Exception:
        pass
    return rows

ape_rows = _read_apewisdom_redis()

import pandas as pd  # already available via db imports but safe to re-import

ape_df = pd.DataFrame(ape_rows) if ape_rows else pd.DataFrame(
    columns=["ticker", "source", "mentions"]
)

# Combine both sources
combined_df = pd.concat([src_ticker_df, ape_df], ignore_index=True)
combined_df = (
    combined_df.groupby(["ticker", "source"], as_index=False)["mentions"]
    .sum()
    .sort_values("mentions", ascending=False)
)

if combined_df.empty:
    st.info(f"No mention data available")
else:
    pivot = (
        combined_df
        .pivot_table(index="ticker", columns="source", values="mentions",
                     aggfunc="sum", fill_value=0)
        .rename_axis(None, axis=1)
        .reset_index()
    )
    pivot["_total"] = pivot.drop(columns=["ticker"]).sum(axis=1)
    pivot = pivot.sort_values("_total", ascending=False).drop(columns=["_total"])

    sources = [c for c in pivot.columns if c != "ticker"]
    fig5 = go.Figure()
    for src in sources:
        fig5.add_trace(go.Bar(
            name=src,
            x=pivot["ticker"].head(30),
            y=pivot[src].head(30),
            hovertemplate=f"<b>%{{x}}</b><br>{src}: %{{y}}<extra></extra>",
        ))
    fig5.update_layout(
        barmode="stack",
        title=f"Mentions per ticker by source — top 30 (text sources: last {hours}h; ApeWisdom: latest 24h count)",
        height=420,
        margin={"t": 55, "b": 20, "l": 10, "r": 10},
        xaxis_title=None,
        yaxis_title="Mentions",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.01,
                "xanchor": "right", "x": 1},
    )
    st.plotly_chart(fig5, use_container_width=True)

    with st.expander("Full breakdown table"):
        st.dataframe(
            combined_df.sort_values(["ticker", "source"]),
            use_container_width=True,
            hide_index=True,
        )
