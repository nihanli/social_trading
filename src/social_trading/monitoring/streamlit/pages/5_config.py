"""
Page 5 — System Configuration.

Full control panel for all SystemConfig parameters, organised into five tabs:

  📋 Watchlist  — active ticker list, pin/unpin, expiry & size limits
  📡 Sources    — discovery & social sources, poll intervals, X API opt-in, on/off toggles
  📊 Signal     — spike detection, two-phase thresholds, factor weights, quality filters
  💼 Position   — sizing, Kelly fraction, volatility target, execution gates
  🛡️ Risk       — exit rules, loss limits, VIX regime thresholds

Changes are written to Redis and picked up by all services within one loop cycle
(~1 minute) — no service restarts required.

Design reference: docs/design/15-ui-monitoring.md §15b, docs/design/16-system-config.md
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../../../"))

import streamlit as st

from social_trading.config.system_config import SystemConfig
from social_trading.monitoring.streamlit.utils.redis_ctrl import (
    clear_watchlist,
    get_pinned_tickers,
    get_source_enabled_states,
    get_source_registry,
    get_watchlist,
    load_config,
    pin_ticker,
    save_config,
    set_source_enabled,
    unpin_ticker,
)

st.set_page_config(page_title="Configuration", page_icon="⚙️", layout="wide")
st.title("⚙️ System Configuration")
st.caption("Changes take effect within one service loop cycle (~1 min). No service restarts required.")

cfg = load_config()

# Human-readable metadata for each ingest source name.
_SOURCE_META: dict[str, dict] = {
    "bluesky":       {"label": "Bluesky",            "desc": "AT Protocol social feed. Free bsky.app account + app password required."},
    "stocktwits":    {"label": "StockTwits",          "desc": "Public StockTwits trending API. No API key required."},
    "apewisdom":     {"label": "ApeWisdom",           "desc": "Aggregated Reddit/social mention leaderboard. No API key required."},
    "yfinance":      {"label": "Yahoo Finance",       "desc": "most_actives / day_gainers / day_losers screeners. No API key required."},
    "alpha_vantage": {"label": "Alpha Vantage",       "desc": "Top gainers/losers endpoint. Free API key required (25 req/day limit)."},
    "ibkr":          {"label": "IBKR Market Scanner", "desc": "Real-time IBKR scanner for high-volume movers. Requires TWS/Gateway connection."},
    "google_trends": {"label": "Google Trends",       "desc": "Trending search interest for financial tickers via pytrends. No API key required."},
    "reddit":        {"label": "Reddit",              "desc": "PRAW streaming source for r/wallstreetbets and finance subreddits. Requires REDDIT_CLIENT_ID in .env."},
    "twitter":       {"label": "X (Twitter)",         "desc": "Paid Tier-2 enrichment source. Controlled by the X API toggle in the Sources tab."},
}
# Mention-history Tier-1 sources — disabling these affects Z-score baselines.
_ZSCORE_SOURCES = {"bluesky", "stocktwits", "apewisdom"}

tab_wl, tab_src, tab_sig, tab_pos, tab_risk = st.tabs([
    "📋 Watchlist",
    "📡 Sources",
    "📊 Signal",
    "💼 Position",
    "🛡️ Risk",
])

# ═══════════════════════════════════════════════════════════════
# TAB 1 — WATCHLIST
# ═══════════════════════════════════════════════════════════════
with tab_wl:
    col_wl, col_seed = st.columns(2)

    with col_wl:
        st.subheader("Active Watchlist")
        watchlist = get_watchlist()
        pinned = get_pinned_tickers()

        pinned_in_list   = sorted(t for t in watchlist if t in pinned)
        unpinned_in_list = sorted(t for t in watchlist if t not in pinned)
        ordered = [f"{t} *" for t in pinned_in_list] + unpinned_in_list

        st.write(f"**{len(watchlist)} tickers currently monitored**")
        if ordered:
            st.caption("\\* = pinned (never auto-expires)")
            st.dataframe({"Ticker": ordered}, width="stretch", hide_index=True)
        else:
            st.info("Watchlist is empty — run seed_watchlist.py or pin tickers below.")

        st.markdown("---")
        st.caption("⚠️ Clear removes all non-pinned tickers and flushes candidates.")
        if st.button("🗑️ Clear Watchlist", type="secondary"):
            st.session_state["_confirm_clear_wl"] = True

        if st.session_state.get("_confirm_clear_wl"):
            st.warning("This will remove all non-pinned tickers. Pinned seeds are kept.")
            col_yes, col_no = st.columns(2)
            if col_yes.button("Yes, clear it", type="primary"):
                removed = clear_watchlist()
                st.session_state.pop("_confirm_clear_wl", None)
                st.success(f"Cleared {removed} ticker(s). Pinned seeds retained.")
                st.rerun()
            if col_no.button("Cancel"):
                st.session_state.pop("_confirm_clear_wl", None)
                st.rerun()

    with col_seed:
        st.subheader("Pin / Unpin Tickers")
        st.caption("Pinned tickers are added immediately and never auto-expired.")
        new_pin = st.text_input("Pin a ticker", placeholder="e.g. NVDA").upper().strip()
        if st.button("Pin Ticker") and new_pin:
            pin_ticker(new_pin)
            st.success(f"{new_pin} pinned to watchlist")
            st.rerun()

        unpin_input = st.text_input("Unpin a ticker", placeholder="e.g. AAPL").upper().strip()
        if st.button("Unpin Ticker") and unpin_input:
            unpin_ticker(unpin_input)
            st.warning(f"{unpin_input} removed from watchlist")
            st.rerun()

    st.divider()
    st.subheader("Watchlist Parameters")
    col_w1, col_w2, col_w3 = st.columns(3)

    with col_w1:
        cfg.watchlist_stale_hours = st.number_input(
            "Stale expiry (hours)", 6, 168, int(cfg.watchlist_stale_hours), 6,
            help=(
                "A ticker is removed from the active watchlist if it has not generated "
                "any social-media mentions within this many hours. Pinned tickers are exempt. "
                "Default: 48 h."
            ),
        )
        cfg.watchlist_max_size = st.number_input(
            "Max watchlist size", 10, 500, int(cfg.watchlist_max_size), 10,
            help=(
                "Hard cap on the number of tickers in the active watchlist. "
                "When the cap is reached, the least-recently-mentioned unpinned ticker "
                "is evicted to make room. Pinned seeds are exempt and do not count toward "
                "this limit. Default: 50."
            ),
        )
    with col_w2:
        cfg.watchlist_min_adv_usd = st.number_input(
            "Min ADV to join watchlist ($)", 100_000, 5_000_000,
            int(cfg.watchlist_min_adv_usd), 100_000, format="%d",
            help=(
                "Average daily dollar volume (ADV) required before a discovered ticker is "
                "promoted from candidate to the active watchlist. Filters out illiquid stocks "
                "that cannot be executed at meaningful size. Default: $500K."
            ),
        )
        cfg.watchlist_min_mcap_usd = st.number_input(
            "Min market cap to join watchlist ($)", 10_000_000, 1_000_000_000,
            int(cfg.watchlist_min_mcap_usd), 10_000_000, format="%d",
            help=(
                "Minimum market capitalisation required for a candidate to be promoted. "
                "Prevents micro-cap pump-and-dump exposure. Default: $50M."
            ),
        )
    with col_w3:
        cfg.watchlist_max_spread_pct = st.slider(
            "Max spread to join watchlist (%)", 0.001, 0.05,
            float(cfg.watchlist_max_spread_pct), 0.001,
            format="%.3f",
            help=(
                "Maximum bid-ask spread (as a fraction of mid price) allowed when a candidate "
                "is evaluated for watchlist promotion. Wide-spread stocks are expensive to trade "
                "and are rejected. Default: 1% (0.01)."
            ),
        )
        cfg.watchlist_promote_interval = st.select_slider(
            "Promotion check interval (sec)",
            options=[60, 120, 300, 600, 900, 1800],
            value=min(
                [60, 120, 300, 600, 900, 1800],
                key=lambda x: abs(x - int(cfg.watchlist_promote_interval)),
            ),
            format_func=lambda s: f"{s // 60} min" if s >= 60 else f"{s}s",
            help=(
                "How often the watchlist manager runs its liquidity gate and promotes "
                "qualifying candidates to the active list. Shorter = faster response but "
                "more frequent IEX/yfinance calls. Default: 10 min."
            ),
        )

# ═══════════════════════════════════════════════════════════════
# TAB 2 — SOURCES
# ═══════════════════════════════════════════════════════════════
with tab_src:
    st.subheader("Discovery Sources")
    st.caption(
        "Discovery sources propose new watchlist candidates by scanning for high-volume / "
        "trending tickers. They run on the discovery poll interval below."
    )

    cfg.discovery_poll_interval_sec = st.select_slider(
        "Discovery poll interval",
        options=[60, 120, 300, 600, 900],
        value=min(
            [60, 120, 300, 600, 900],
            key=lambda x: abs(x - int(cfg.discovery_poll_interval_sec)),
        ),
        format_func=lambda s: f"{s // 60} min" if s >= 60 else f"{s}s",
        help=(
            "How often Yahoo Finance, Alpha Vantage, IBKR Scanner, and Google Trends "
            "run their discovery cycle to propose new watchlist candidates. "
            "Shorter = faster discovery but higher API cost (especially Alpha Vantage, "
            "which has a 25 req/day free quota). Default: 5 min."
        ),
    )

    col_yf, col_av = st.columns(2)
    with col_yf:
        st.markdown("**Yahoo Finance Screener**")
        st.caption("No API key required. Queries most_actives, day_gainers, day_losers.")
        cfg.yfinance_screener_count = st.slider(
            "Tickers per screener", 10, 100, int(cfg.yfinance_screener_count), 10,
            help=(
                "Number of results fetched from each of the three Yahoo Finance screeners "
                "(most_actives, day_gainers, day_losers) per discovery cycle. "
                "The 3 screener results are de-duplicated before candidate proposals. "
                "Higher count = broader discovery but more downstream lookups. Default: 50."
            ),
            key="yf_count",
        )
    with col_av:
        st.markdown("**Alpha Vantage**")
        st.caption("Free API key required (ALPHA_VANTAGE_API_KEY). Limit: 25 req/day.")
        cfg.alpha_vantage_cache_ttl_sec = st.select_slider(
            "Cache TTL",
            options=[900, 1800, 3600, 7200, 14400],
            value=min(
                [900, 1800, 3600, 7200, 14400],
                key=lambda x: abs(x - int(cfg.alpha_vantage_cache_ttl_sec)),
            ),
            format_func=lambda s: f"{s // 3600}h" if s >= 3600 else f"{s // 60}m",
            help=(
                "How long to cache the TOP_GAINERS_LOSERS response from Alpha Vantage. "
                "The free tier allows only 25 requests per day, so the cache TTL directly "
                "controls how many real API calls are made. "
                "Shorter = fresher data, faster quota burn. Default: 1 h."
            ),
            key="av_ttl",
        )

    st.info(
        "**IBKR Market Scanner** and **Google Trends** connection settings are read from `.env`. "
        "Set `IBKR_SCANNER_PORT`, `IBKR_SCANNER_CLIENT_ID` for IBKR; "
        "install `pytrends` for Google Trends.",
        icon="ℹ️",
    )

    st.divider()
    st.subheader("Social / Sentiment Sources")
    st.caption(
        "Social sources poll the active watchlist for mention counts and sentiment posts. "
        "They run on the social poll interval below and feed the Z-score spike detector."
    )

    col_st1, col_st2 = st.columns(2)
    with col_st1:
        cfg.stocktwits_poll_interval_sec = st.select_slider(
            "Social poll interval",
            options=[60, 120, 300, 600],
            value=min(
                [60, 120, 300, 600],
                key=lambda x: abs(x - int(cfg.stocktwits_poll_interval_sec)),
            ),
            format_func=lambda s: f"{s // 60} min" if s >= 60 else f"{s}s",
            help=(
                "How often StockTwits, Bluesky, and ApeWisdom poll the active watchlist "
                "for new mentions. These feeds drive the mention-count Z-score that detects "
                "social spikes. Shorter = lower latency but more requests. Default: 5 min."
            ),
        )
    with col_st2:
        cfg.bluesky_search_count = st.slider(
            "Bluesky posts per search", 10, 100, int(cfg.bluesky_search_count), 5,
            help=(
                "Maximum number of posts to fetch per ticker per Bluesky search call. "
                "Bluesky is free with no per-request cost, so this can be set generously. "
                "Higher values give better sentiment coverage but use more memory. Default: 25."
            ),
        )

    st.divider()
    st.subheader("X (Twitter) API — disabled by default")

    with st.expander("⚠️  X (Twitter) API — pay-per-use, disabled by default"):
        st.warning(
            "X API is now **pay-per-use** with no free tier. Enabling this with 50 tickers "
            "at 5-min intervals costs approximately **$72/day** ($2,160/month) for Counts "
            "alone. StockTwits + Bluesky provide equivalent spike detection at zero cost."
        )
        cfg.x_api_enabled = st.toggle(
            "Enable X API (requires X_BEARER_TOKEN in .env)",
            value=bool(cfg.x_api_enabled),
            help=(
                "When enabled, tickers that pass Phase-1 scoring trigger a paid X API "
                "search to enrich the signal with real-time tweet sentiment before the "
                "final Phase-2 threshold check. Only enable if you have a paid X developer "
                "plan and have set X_BEARER_TOKEN in .env."
            ),
        )
        if cfg.x_api_enabled:
            col_xa, col_xb = st.columns(2)
            with col_xa:
                cfg.x_search_max_results = st.slider(
                    "Posts fetched per spike", 10, 100, int(cfg.x_search_max_results), 10,
                    help=(
                        f"Number of tweets pulled for each Phase-1 ticker via the X API. "
                        f"At $0.005/request this setting costs approximately "
                        f"${cfg.x_search_max_results * 0.005:.2f} per enrichment call. "
                        "More posts = richer sentiment sample but higher cost. Default: 100."
                    ),
                    key="x_max_results",
                )
            with col_xb:
                cfg.counts_poll_interval_sec = st.select_slider(
                    "X Counts poll interval",
                    options=[60, 120, 300, 600],
                    value=min(
                        [60, 120, 300, 600],
                        key=lambda x: abs(x - int(cfg.counts_poll_interval_sec)),
                    ),
                    format_func=lambda s: f"{s // 60} min" if s >= 60 else f"{s}s",
                    help=(
                        "How often the X Counts API is polled per ticker for volume trending. "
                        "This is billed per request, so reducing the interval raises cost linearly. "
                        "Default: 5 min."
                    ),
                    key="x_poll_interval",
                )

    st.divider()
    st.subheader("Runtime Source On/Off Controls")
    st.caption(
        "Enable or disable individual sources at runtime — no restart required. "
        "Changes take effect on the next poll cycle (~30 s)."
    )

    source_registry = get_source_registry()
    enabled_states  = get_source_enabled_states()

    if not source_registry:
        st.info(
            "No sources registered yet. The ingest service publishes this list at startup — "
            "start ingest_service and reload this page.",
            icon="ℹ️",
        )
    else:
        polling_tier1 = {
            name: meta for name, meta in source_registry.items()
            if not meta.get("streaming") and meta.get("tier", 1) == 1 and name != "twitter"
        }
        streaming_sources = {
            name: meta for name, meta in source_registry.items()
            if meta.get("streaming")
        }
        tier2_sources = {
            name: meta for name, meta in source_registry.items()
            if meta.get("tier", 1) == 2
        }

        if polling_tier1:
            st.markdown("**Tier-1 Polling Sources** *(toggle takes effect immediately)*")
            for name in sorted(polling_tier1):
                meta       = _SOURCE_META.get(name, {"label": name, "desc": ""})
                is_enabled = enabled_states.get(name, True)
                col_chk, col_info = st.columns([1, 5])
                with col_chk:
                    new_val = st.checkbox(
                        meta["label"],
                        value=is_enabled,
                        key=f"src_enabled_{name}",
                    )
                    if new_val != is_enabled:
                        set_source_enabled(name, new_val)
                        st.rerun()
                with col_info:
                    st.caption(meta["desc"])
                    if not is_enabled and name in _ZSCORE_SOURCES:
                        st.warning(
                            f"⚠️ {meta['label']} feeds the mention-history Z-score. "
                            "Disabling it may bias spike detection until existing history "
                            "entries expire (~1 h).",
                            icon="⚠️",
                        )

        if streaming_sources or tier2_sources:
            st.markdown("**Read-only sources** *(require restart to change)*")
            for name in sorted({**streaming_sources, **tier2_sources}):
                meta = _SOURCE_META.get(name, {"label": name, "desc": ""})
                note = (
                    "🔄 Streaming source — disable by removing credentials from .env and restarting ingest_service."
                    if source_registry.get(name, {}).get("streaming")
                    else "💳 Tier-2 paid source — controlled by the X API toggle above."
                )
                st.markdown(f"**{meta['label']}** — {meta['desc']}")
                st.caption(note)

# ═══════════════════════════════════════════════════════════════
# TAB 3 — SIGNAL
# ═══════════════════════════════════════════════════════════════
with tab_sig:
    st.subheader("Spike Detection")
    col_sd1, col_sd2 = st.columns(2)

    with col_sd1:
        cfg.spike_zscore_threshold = st.slider(
            "Spike Z-score threshold", 1.0, 4.0, float(cfg.spike_zscore_threshold), 0.1,
            help=(
                "Minimum Z-score of the current mention count (relative to the rolling "
                "baseline) required to classify an event as a social spike. "
                "Z = (current_count − mean) / std_dev. "
                "Higher = fewer but higher-conviction spikes. Lower = more signals, more noise. "
                "Typical range: 1.5 (sensitive) – 3.0 (selective). Default: 2.0."
            ),
        )
        cfg.mention_window_minutes = st.number_input(
            "Mention count window (min)", 15, 240, int(cfg.mention_window_minutes), 15,
            help=(
                "Rolling time window used to count social-media mentions when computing "
                "the Z-score. A wider window smooths noise but reduces responsiveness "
                "to fast spikes. Default: 60 min."
            ),
        )
    with col_sd2:
        cfg.signal_age_max_hours = st.number_input(
            "Max signal age (hours)", 1, 96, int(cfg.signal_age_max_hours), 1,
            help=(
                "Signals older than this are discarded before they reach the risk service. "
                "Prevents stale high-Z tickers from generating trades hours after the spike "
                "has passed. Default: 48 h."
            ),
        )
        cfg.signal_approval_max_age_min = st.number_input(
            "Max signal age at approval (min)", 1, 60,
            int(cfg.signal_approval_max_age_min), 1,
            help=(
                "Hard freshness gate at risk-service approval time. If a signal was generated "
                "more than this many minutes ago (e.g. due to queue backlog), it is rejected "
                "as stale and no order is placed. Prevents acting on outdated momentum. "
                "Default: 10 min."
            ),
        )

    st.divider()
    st.subheader("Two-Phase Thresholds")
    st.caption(
        "Phase 1 uses free Tier-1 sources (always on). "
        "When X/Twitter API is enabled, tickers passing Phase 1 trigger paid Tier-2 enrichment "
        "and are re-evaluated against the stricter Phase 2 threshold. "
        "With no paid sources configured, Phase 1 signals fire directly."
    )
    col_p1, col_p2 = st.columns(2)

    with col_p1:
        cfg.signal_phase1_threshold = st.slider(
            "Phase 1 threshold (Tier-1 sources only)", 0.2, 0.95,
            float(cfg.signal_phase1_threshold), 0.01,
            help=(
                "Composite signal score (0–1) required to pass the first evaluation phase. "
                "Scores are a weighted sum of volume Z-score, sentiment strength, proactivity, "
                "price momentum, and cross-platform convergence factors. "
                "Signals above this value fire directly (no paid API) or trigger Tier-2 enrichment "
                "when X API is enabled. Default: 0.40."
            ),
        )
        cfg.signal_phase2_threshold = st.slider(
            "Phase 2 threshold (Tier-1 + Tier-2 sources)", 0.2, 0.95,
            float(cfg.signal_phase2_threshold), 0.01,
            help=(
                "Stricter composite score threshold applied after Tier-2 (X API) enrichment. "
                "Only signals that re-score above this level proceed to execution. "
                "Should be higher than Phase 1 to justify the API cost. Default: 0.65."
            ),
        )
    with col_p2:
        cfg.phase2_max_tickers_per_cycle = st.number_input(
            "Max Tier-2 enrichment calls per cycle", 1, 50,
            int(cfg.phase2_max_tickers_per_cycle), 1,
            help=(
                "Hard cap on the number of Tier-2 (X API) enrichment calls per signal "
                "evaluation cycle. Acts as a cost guardrail — even if many tickers pass "
                "Phase 1 simultaneously, at most this many will trigger a paid API call. "
                "Default: 10."
            ),
        )
        cfg.phase2_skip_open_positions = st.checkbox(
            "Skip enrichment for tickers with open positions",
            value=bool(cfg.phase2_skip_open_positions),
            help=(
                "When enabled, tickers that already have an open position are not submitted "
                "for Tier-2 enrichment. Avoids paying for X API data when a trade is already "
                "in progress and a new signal would be blocked by the duplicate-position guard. "
                "Default: enabled."
            ),
        )

    st.divider()
    col_fw, col_qf = st.columns(2)

    with col_fw:
        st.subheader("Factor Weights")
        st.caption("Must sum to exactly 1.00.")
        cfg.w_volume = st.slider(
            "Volume Z-score weight", 0.0, 0.6, float(cfg.w_volume), 0.05,
            help=(
                "Weight of the mention-volume Z-score factor in the composite signal score. "
                "This factor measures how many standard deviations above baseline the current "
                "mention count is. Higher weight = stronger emphasis on raw volume spikes. "
                "Default: 0.30."
            ),
        )
        cfg.w_sentiment = st.slider(
            "Sentiment strength weight", 0.0, 0.6, float(cfg.w_sentiment), 0.05,
            help=(
                "Weight of the aggregated post sentiment (VADER/FinBERT compound score) "
                "in the composite signal. Positive sentiment boosts the score; negative "
                "sentiment suppresses it. Higher weight = sentiment dominates. Default: 0.25."
            ),
        )
        cfg.w_proactivity = st.slider(
            "Proactivity weight", 0.0, 0.5, float(cfg.w_proactivity), 0.05,
            help=(
                "Weight of the proactivity factor, which rewards signals where social mention "
                "activity precedes price movement ('leading') and penalises signals that follow "
                "an existing price move ('reactive'). Default: 0.20."
            ),
        )
        cfg.w_momentum = st.slider(
            "Price momentum weight", 0.0, 0.4, float(cfg.w_momentum), 0.05,
            help=(
                "Weight of the short-term price momentum factor. A small positive price move "
                "concurrent with the social spike adds conviction; a large pre-existing move "
                "reduces it (reactive signal penalty). Default: 0.15."
            ),
        )
        cfg.w_convergence = st.slider(
            "Cross-platform convergence weight", 0.0, 0.3, float(cfg.w_convergence), 0.05,
            help=(
                "Weight of the cross-platform signal convergence bonus. Applied when multiple "
                "independent sources (e.g. StockTwits + Bluesky + ApeWisdom) spike for the "
                "same ticker at the same time, increasing confidence the signal is genuine. "
                "Default: 0.10."
            ),
        )
        weight_sum = (
            cfg.w_volume + cfg.w_sentiment + cfg.w_proactivity
            + cfg.w_momentum + cfg.w_convergence
        )
        color = "green" if abs(weight_sum - 1.0) < 0.01 else "red"
        st.markdown(f"**Weight sum: :{color}[{weight_sum:.2f}]** (must equal 1.00)")

    with col_qf:
        st.subheader("Quality Filters")
        cfg.sentiment_strength_min = st.slider(
            "Min |sentiment| to fire", 0.1, 0.8, float(cfg.sentiment_strength_min), 0.05,
            help=(
                "Minimum absolute sentiment compound score required for a signal to proceed. "
                "Posts near zero are weakly opinionated; this filter discards ambiguous signals "
                "where the crowd has no clear bullish or bearish conviction. "
                "Applies to the aggregated score across all posts in the mention window. Default: 0.30."
            ),
        )
        cfg.price_momentum_min_pct = st.slider(
            "Min price move for momentum factor (%)", 0.0, 0.10,
            float(cfg.price_momentum_min_pct), 0.005, format="%.3f",
            help=(
                "Minimum intraday price change (as a fraction of open) required to contribute "
                "a positive momentum factor. Below this threshold the momentum contribution is "
                "neutral (0). Default: 2% (0.02)."
            ),
        )
        cfg.reactive_price_threshold = st.slider(
            "Reactive price threshold (%)", 0.05, 0.25, float(cfg.reactive_price_threshold), 0.01,
            help=(
                "If the price has already moved more than this fraction before the social spike "
                "is detected, the signal is classified as 'reactive' (chasing) and receives a "
                "proactivity penalty. Lower = penalise smaller prior moves. Default: 10% (0.10)."
            ),
        )
        cfg.convergence_bonus = st.slider(
            "Cross-platform convergence bonus", 0.0, 0.5, float(cfg.convergence_bonus), 0.05,
            help=(
                "Additive bonus applied to the signal score when multiple independent sources "
                "converge on the same ticker in the same window. For example, if both StockTwits "
                "and Bluesky spike for NVDA simultaneously the score receives this bonus. "
                "Default: 0.20."
            ),
        )
        cfg.signal_decay_lambda = st.slider(
            "Signal decay λ", 0.01, 0.5, float(cfg.signal_decay_lambda), 0.01,
            help=(
                "Hyperbolic decay rate applied to signal scores as they age. "
                "score(t) = score₀ / (1 + λ × t_hours). "
                "Higher λ = faster decay (signal loses value more quickly). "
                "At λ=0.10 a score halves after ~7 hours. Default: 0.10."
            ),
        )

# ═══════════════════════════════════════════════════════════════
# TAB 4 — POSITION
# ═══════════════════════════════════════════════════════════════
with tab_pos:
    st.subheader("Position Sizing")
    col_ps1, col_ps2, col_ps3 = st.columns(3)

    with col_ps1:
        cfg.max_position_pct = st.slider(
            "Max position size (% of NLV)", 0.5, 5.0,
            float(cfg.max_position_pct * 100), 0.25,
            help=(
                "Maximum capital allocated to a single trade as a percentage of net "
                "liquidation value (NLV). The Kelly-sized quantity is capped at this limit. "
                "Must be ≤ max_single_position. Default: 2% of NLV."
            ),
        ) / 100
        cfg.half_kelly_fraction = st.slider(
            "Kelly fraction", 0.1, 1.0, float(cfg.half_kelly_fraction), 0.1,
            help=(
                "Multiplier applied to the full Kelly criterion bet size. "
                "1.0 = full Kelly (maximises log-wealth but very aggressive). "
                "0.5 = half Kelly (recommended — reduces drawdown risk significantly). "
                "0.25 = quarter Kelly (very conservative). Default: 0.5."
            ),
        )
        cfg.sigma_target = st.slider(
            "Target annual volatility", 0.05, 0.40, float(cfg.sigma_target), 0.01,
            help=(
                "Target annualised volatility used for volatility-scaled sizing. "
                "When a stock's realised vol exceeds this target, the position is scaled "
                "down proportionally; when it is below, the position is scaled up (but "
                "still capped by max_position_pct). Default: 15%."
            ),
        )

    with col_ps2:
        cfg.max_social_allocation = st.slider(
            "Max total social allocation (% of NLV)", 5.0, 50.0,
            float(cfg.max_social_allocation * 100), 5.0,
            help=(
                "Maximum combined portfolio exposure across all social-signal positions. "
                "When the total mark-to-market value of open social trades reaches this "
                "fraction of NLV, no new positions are opened. Prevents overconcentration "
                "in the social-momentum theme. Default: 20% of NLV."
            ),
        ) / 100
        cfg.max_sector_allocation = st.slider(
            "Max sector allocation (% of NLV)", 5.0, 50.0,
            float(cfg.max_sector_allocation * 100), 5.0,
            help=(
                "Maximum combined exposure to any single GICS sector (e.g. Technology, "
                "Energy). Prevents the portfolio from being overweight a single sector "
                "if multiple social signals hit the same industry simultaneously. Default: 15%."
            ),
        ) / 100
        cfg.max_single_position = st.slider(
            "Max single position (% of NLV)", 2.0, 25.0,
            float(cfg.max_single_position * 100), 1.0,
            help=(
                "Absolute cap on any single ticker's position as a percentage of NLV. "
                "Acts as a backstop to max_position_pct — used in validation to prevent "
                "misconfiguration. Must be ≥ max_position_pct. Default: 10%."
            ),
        ) / 100

    with col_ps3:
        cfg.trade_max_spread_bps = st.number_input(
            "Max bid-ask spread at execution (bps)", 10, 500,
            int(cfg.trade_max_spread_bps), 10,
            help=(
                "Maximum allowed bid-ask spread in basis points (1 bps = 0.01%) at the "
                "moment an order is submitted. If the live spread exceeds this gate, the "
                "order is held until the spread tightens or the signal expires. "
                "Prevents entering positions with high implicit transaction cost. Default: 100 bps (1%)."
            ),
        )
        cfg.trade_min_adv_usd = st.number_input(
            "Min ADV at execution ($)", 100_000, 10_000_000,
            int(cfg.trade_min_adv_usd), 100_000, format="%d",
            help=(
                "Minimum average daily dollar volume (ADV) required at order submission time. "
                "A separate, real-time check from the watchlist ADV gate — the execution ADV "
                "can be set higher since we need confidence we can fill and exit without "
                "meaningful market impact. Default: $500K."
            ),
        )
        cfg.trade_max_order_adv_pct = st.slider(
            "Max order size (% of ADV)", 0.001, 0.05,
            float(cfg.trade_max_order_adv_pct), 0.001, format="%.3f",
            help=(
                "Maximum order size as a fraction of average daily volume. "
                "Prevents the order from being a meaningful fraction of the day's volume, "
                "which would move the market and cause slippage. "
                "0.005 = 0.5% of ADV (default). "
                "At $1M ADV this caps the order at $5,000 notional."
            ),
        )

# ═══════════════════════════════════════════════════════════════
# TAB 5 — RISK
# ═══════════════════════════════════════════════════════════════
with tab_risk:
    st.subheader("Exit Rules")
    col_ex1, col_ex2, col_ex3 = st.columns(3)

    with col_ex1:
        cfg.take_profit_pct = st.slider(
            "Take profit (%)", 1.0, 15.0, float(cfg.take_profit_pct * 100), 0.5,
            help=(
                "Target price gain at which an OCA limit-sell order fires to close the position "
                "and lock in profit. Set relative to the fill price at entry. "
                "E.g. 4% means a limit sell is placed 4% above the entry fill. Default: 4%."
            ),
        ) / 100
        cfg.trailing_stop_pct = st.slider(
            "Trailing stop (%)", 2.0, 20.0, float(cfg.trailing_stop_pct * 100), 0.5,
            help=(
                "Trailing stop distance from the position's high-water mark. "
                "The stop moves up with price but never down. If price retraces "
                "this percentage from the peak, the stop triggers a market sell. "
                "Only activates once the position is in profit by the activation "
                "threshold below. Default: 8%."
            ),
        ) / 100
        cfg.trailing_stop_activation_pct = st.slider(
            "Trailing stop activation (%)", 0.0, 5.0, float(cfg.trailing_stop_activation_pct * 100), 0.25,
            help=(
                "The trailing stop only activates once the position has moved this "
                "far into profit from entry. This prevents the trailing stop from "
                "acting as a parallel stop-loss on positions that never became "
                "profitable — the ATR stop handles those. Default: 1%."
            ),
        ) / 100
        cfg.atr_multiplier = st.slider(
            "ATR stop multiplier", 0.5, 5.0, float(cfg.atr_multiplier), 0.25,
            help=(
                "Initial stop-loss distance set as N × ATR (Average True Range) from the "
                "entry price. ATR captures the stock's typical daily range, so the stop "
                "adapts to each stock's volatility. A tighter multiplier (e.g. 1.0) is "
                "faster but risks being stopped out by normal noise. Default: 2× ATR."
            ),
        )

    with col_ex2:
        cfg.max_hold_hours = st.number_input(
            "Max hold time (hours)", 4, 120, int(cfg.max_hold_hours), 4,
            help=(
                "Hard time-based exit: a position is closed after it has been open for "
                "this many hours regardless of PnL. Prevents indefinite holding of stale "
                "social-momentum trades that were never stopped out or taken profit. "
                "Default: 48 h."
            ),
        )
        cfg.signal_reversal_threshold = st.slider(
            "Sentiment reversal threshold", -0.8, -0.05,
            float(cfg.signal_reversal_threshold), 0.05,
            help=(
                "Sentiment-based exit: the position is closed when the aggregated "
                "sentiment score for the ticker drops below this value. A value of -0.20 "
                "means the crowd has turned clearly bearish. More negative = requires "
                "stronger reversal to exit; less negative = exits on mild negativity. "
                "Default: -0.20."
            ),
        )

    with col_ex3:
        cfg.mention_decay_threshold = st.slider(
            "Mention decay exit (fraction of peak)", 0.05, 0.5,
            float(cfg.mention_decay_threshold), 0.05,
            help=(
                "Mention-decay exit: position is closed when the smoothed current mention "
                "count falls below this fraction of the peak count seen since entry. "
                "E.g. 0.25 means exit when mentions drop to 25% of peak — the crowd has "
                "lost interest. Lower = hold longer; higher = exit earlier. Default: 0.25."
            ),
        )
        cfg.mention_decay_min_hold_hours = st.slider(
            "Mention decay min hold (hours)", 0.25, 8.0,
            float(cfg.mention_decay_min_hold_hours), 0.25,
            help=(
                "Minimum time a position must be held before the MENTION_DECAY exit rule "
                "can fire. Prevents the spike that triggered entry from immediately triggering "
                "the decay exit, since mention count naturally dips right after a peak. "
                "Default: 1 h."
            ),
        )
        cfg.mention_decay_smooth_samples = st.slider(
            "Mention decay smooth samples", 1, 12,
            int(cfg.mention_decay_smooth_samples), 1,
            help=(
                "Number of recent poll windows to average when computing the current "
                "mention ratio for the decay exit. Each poll window is ~5 min, so "
                "3 samples = ~15 min smoothing, 6 = ~30 min. "
                "Higher = less noise but slower response to genuine decay. Default: 3."
            ),
        )

    st.divider()
    col_rl, col_vix = st.columns(2)

    with col_rl:
        st.subheader("Loss Limits")
        cfg.loss_limit_single_trade = st.slider(
            "Single trade loss limit (%)", 0.5, 5.0,
            float(cfg.loss_limit_single_trade * 100), 0.25,
            help=(
                "Per-position emergency stop: if unrealised loss on any single trade reaches "
                "this percentage of NLV, the position is closed immediately regardless of "
                "other exit rules. This is the last-resort circuit breaker for a position gone "
                "badly wrong. Default: 1% of NLV."
            ),
        ) / 100
        cfg.loss_limit_daily = st.slider(
            "Daily loss limit (%) — halts new trades", 1.0, 10.0,
            float(cfg.loss_limit_daily * 100), 0.5,
            help=(
                "If realised + unrealised losses for the current trading day reach this "
                "percentage of NLV, the system stops opening new positions for the rest of "
                "the day. Existing positions continue to be managed. Default: 3% of NLV."
            ),
        ) / 100
        cfg.loss_limit_weekly = st.slider(
            "Weekly loss limit (%) — reduces sizes 50%", 2.0, 20.0,
            float(cfg.loss_limit_weekly * 100), 0.5,
            help=(
                "If cumulative weekly losses reach this percentage of NLV, position sizes "
                "are automatically halved for the remainder of the week. Allows continued "
                "trading but at reduced risk. Default: 7% of NLV."
            ),
        ) / 100
        cfg.loss_limit_monthly = st.slider(
            "Monthly loss limit (%) — warning threshold", 5.0, 30.0,
            float(cfg.loss_limit_monthly * 100), 1.0,
            help=(
                "Monthly cumulative loss level that triggers a warning alert. "
                "Does not automatically halt trading but surfaces prominently in the "
                "dashboard. Useful for manual intervention. Default: 15% of NLV."
            ),
        ) / 100
        cfg.drawdown_halt = st.slider(
            "Max drawdown from HWM (%) — full halt", 5.0, 40.0,
            float(cfg.drawdown_halt * 100), 1.0,
            help=(
                "If the portfolio drawdown from its all-time high-water mark (HWM) reaches "
                "this percentage, all new trading is halted and all open positions are closed. "
                "This is the ultimate circuit breaker for sustained capital loss. "
                "Default: 20% drawdown from HWM."
            ),
        ) / 100

    with col_vix:
        st.subheader("VIX Regime Thresholds")
        st.caption(
            "Position size scalars applied based on the current VIX level. "
            "As market fear rises, sizes are progressively reduced: "
            "100% → 75% → 50% → 25% → 0%."
        )
        cfg.vix_slightly_elevated = float(st.number_input(
            "VIX slightly elevated → 75% size", 10, 40,
            int(cfg.vix_slightly_elevated),
            help=(
                "VIX level above which position sizes are scaled to 75% of the full "
                "Kelly-sized quantity. Represents mildly elevated market anxiety. Default: 20."
            ),
        ))
        cfg.vix_elevated = float(st.number_input(
            "VIX elevated → 50% size", 10, 50, int(cfg.vix_elevated),
            help=(
                "VIX level above which position sizes are halved. Represents meaningfully "
                "elevated volatility where drawdown risk increases. Default: 25."
            ),
        ))
        cfg.vix_high_fear = float(st.number_input(
            "VIX high fear → 25% size", 15, 60, int(cfg.vix_high_fear),
            help=(
                "VIX level above which position sizes are reduced to 25% of normal. "
                "Represents high-fear regimes (e.g. VIX > 30 during market stress). Default: 30."
            ),
        ))
        cfg.vix_crisis = float(st.number_input(
            "VIX crisis → 0% size (no new trades)", 20, 80, int(cfg.vix_crisis),
            help=(
                "VIX level above which no new positions are opened. The system stops all "
                "new entries during extreme market dislocations. Existing positions are still "
                "managed. Default: 40."
            ),
        ))

# ═══════════════════════════════════════════════════════════════
# SAVE / RESET  (outside tabs — always visible)
# ═══════════════════════════════════════════════════════════════
st.divider()
col_save, col_reset = st.columns([3, 1])

with col_save:
    if st.button("💾 Save Configuration", type="primary", width='stretch'):
        errors = save_config(cfg)
        if errors:
            for e in errors:
                st.error(e)
        else:
            st.success("Configuration saved. All services will pick up changes within ~1 minute.")
            st.balloons()

with col_reset:
    if st.button("↺ Reset to Defaults", width='stretch'):
        errs = save_config(SystemConfig())
        if not errs:
            st.warning("Reset to factory defaults.")
            st.rerun()
