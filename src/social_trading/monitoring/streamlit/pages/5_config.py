"""
Page 5 — System Configuration.

Full control panel for all SystemConfig parameters.
Changes are written to Redis and picked up by all services within one loop cycle
(~1 minute) — no service restarts required.

Sections:
  1. Watchlist management (active tickers, pin/unpin)
  2. Discovery & spike detection thresholds
     2a. X (Twitter) spike detection
     2b. Trending ticker sources (yfinance, Alpha Vantage, IBKR scanner)
  3. Signal quality (threshold + factor weights)
  4. Position sizing
  5. Exit rules
  6. Risk & circuit breakers (loss limits, VIX thresholds)

Design reference: docs/design/15-ui-monitoring.md §15b, docs/design/16-system-config.md
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../../../"))

import streamlit as st

from social_trading.config.system_config import SystemConfig
from social_trading.monitoring.streamlit.utils.redis_ctrl import (
    get_watchlist,
    load_config,
    pin_ticker,
    save_config,
    unpin_ticker,
)

st.set_page_config(page_title="Configuration", page_icon="⚙️", layout="wide")
st.title("System Configuration")
st.caption("Changes take effect within one service loop cycle (~1 min). No restarts required.")

cfg = load_config()

# ═══════════════════════════════════════════════════════════════
# 1. WATCHLIST MANAGEMENT
# ═══════════════════════════════════════════════════════════════
st.header("1. Watchlist Management")
col_wl, col_seed = st.columns(2)

with col_wl:
    st.subheader("Active Watchlist")
    watchlist = get_watchlist()
    st.write(f"**{len(watchlist)} tickers currently monitored**")
    if watchlist:
        st.dataframe({"Ticker": watchlist}, use_container_width=True, hide_index=True)
    else:
        st.info("Watchlist is empty — run seed_watchlist.py or pin tickers below")

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

# ═══════════════════════════════════════════════════════════════
# 2. DISCOVERY & SPIKE DETECTION
# ═══════════════════════════════════════════════════════════════
st.header("2. Discovery & Spike Detection")
col1, col2, col3 = st.columns(3)

with col1:
    cfg.spike_zscore_threshold = st.slider(
        "Spike Z-score threshold", 1.0, 4.0, float(cfg.spike_zscore_threshold), 0.1,
        help="Higher = fewer, stronger signals. Lower = more signals, more noise.",
    )
    cfg.mention_window_minutes = st.number_input(
        "Mention count window (min)", 15, 240, int(cfg.mention_window_minutes), 15,
    )
with col2:
    cfg.x_search_max_results = st.slider(
        "X posts pulled per spike", 10, 100, int(cfg.x_search_max_results), 10,
        help=f"API cost ≈ ${cfg.x_search_max_results * 0.005:.2f} per spike.",
    )
    cfg.counts_poll_interval_sec = st.select_slider(
        "X Counts poll interval (sec)", [60, 120, 300, 600], int(cfg.counts_poll_interval_sec),
    )
with col3:
    cfg.watchlist_stale_hours = st.number_input(
        "Watchlist stale expiry (hours)", 6, 168, int(cfg.watchlist_stale_hours), 6,
    )
    cfg.watchlist_min_adv_usd = st.number_input(
        "Min ADV for watchlist ($)", 100_000, 5_000_000,
        int(cfg.watchlist_min_adv_usd), 100_000, format="%d",
    )

st.divider()

# ═══════════════════════════════════════════════════════════════
# 2b. TRENDING TICKER SOURCES
# ═══════════════════════════════════════════════════════════════
st.subheader("2b. Trending Ticker Sources")
st.caption(
    "Controls for the three supplementary discovery sources that replace StockTwits. "
    "Discovered tickers are proposed to the watchlist and subject to the liquidity gate."
)

cfg.discovery_poll_interval_sec = st.select_slider(
    "Discovery poll interval",
    options=[60, 120, 300, 600, 900],
    value=min(
        [60, 120, 300, 600, 900],
        key=lambda x: abs(x - int(cfg.discovery_poll_interval_sec)),
    ),
    format_func=lambda s: f"{s // 60} min" if s >= 60 else f"{s}s",
    help="How often yfinance, Alpha Vantage, and IBKR scanner run their discovery cycle.",
)

col_yf, col_av = st.columns(2)

with col_yf:
    st.markdown("**Yahoo Finance Screener**")
    st.caption("No API key required. Queries most_actives, day_gainers, day_losers.")
    cfg.yfinance_screener_count = st.slider(
        "Tickers per screener", 10, 100, int(cfg.yfinance_screener_count), 10,
        help="Number of results fetched from each of the 3 Yahoo screeners per cycle.",
        key="yf_count",
    )

with col_av:
    st.markdown("**Alpha Vantage**")
    st.caption("Free API key required (ALPHA_VANTAGE_API_KEY). Limit: 25 req/day.")
    cfg.alpha_vantage_cache_ttl_sec = st.select_slider(
        "Cache TTL (seconds)",
        options=[900, 1800, 3600, 7200, 14400],
        value=min(
            [900, 1800, 3600, 7200, 14400],
            key=lambda x: abs(x - int(cfg.alpha_vantage_cache_ttl_sec)),
        ),
        format_func=lambda s: f"{s // 3600}h" if s >= 3600 else f"{s // 60}m",
        help="How long to cache the TOP_GAINERS_LOSERS response. "
             "Shorter = fresher data but burns daily quota faster.",
        key="av_ttl",
    )

st.info(
    "**IBKR Market Scanner** connection settings (port, client ID) are read from "
    "`.env` — set `IBKR_SCANNER_PORT` and `IBKR_SCANNER_CLIENT_ID` there.",
    icon="ℹ️",
)

st.divider()
# ═══════════════════════════════════════════════════════════════
st.header("3. Signal Quality")
col_s1, col_s2 = st.columns(2)

with col_s1:
    cfg.signal_quality_threshold = st.slider(
        "Minimum signal quality score", 0.3, 0.95, float(cfg.signal_quality_threshold), 0.05,
        help="Signals below this score are discarded.",
    )
    cfg.sentiment_strength_min = st.slider(
        "Min |sentiment| to fire", 0.1, 0.8, float(cfg.sentiment_strength_min), 0.05,
    )
    cfg.reactive_price_threshold = st.slider(
        "Reactive price threshold", 0.05, 0.25, float(cfg.reactive_price_threshold), 0.01,
        help="Price move before mention that classifies signal as 'reactive' (penalised).",
    )

with col_s2:
    st.subheader("Factor Weights")
    st.caption("Must sum to exactly 1.00")
    cfg.w_volume      = st.slider("Volume Z-score weight",     0.0, 0.6, float(cfg.w_volume),      0.05)
    cfg.w_sentiment   = st.slider("Sentiment strength weight", 0.0, 0.6, float(cfg.w_sentiment),   0.05)
    cfg.w_proactivity = st.slider("Proactivity weight",        0.0, 0.5, float(cfg.w_proactivity), 0.05)
    cfg.w_momentum    = st.slider("Price momentum weight",     0.0, 0.4, float(cfg.w_momentum),    0.05)
    cfg.w_convergence = st.slider("Cross-platform weight",     0.0, 0.3, float(cfg.w_convergence), 0.05)
    weight_sum = (
        cfg.w_volume + cfg.w_sentiment + cfg.w_proactivity
        + cfg.w_momentum + cfg.w_convergence
    )
    color = "green" if abs(weight_sum - 1.0) < 0.01 else "red"
    st.markdown(f"**Weight sum: :{color}[{weight_sum:.2f}]** (must equal 1.00)")

st.divider()

# ═══════════════════════════════════════════════════════════════
# 4. POSITION SIZING
# ═══════════════════════════════════════════════════════════════
st.header("4. Position Sizing")
col_p1, col_p2, col_p3 = st.columns(3)

with col_p1:
    cfg.max_position_pct = st.slider(
        "Max position size (%)", 0.5, 5.0, float(cfg.max_position_pct * 100), 0.25,
    ) / 100
    cfg.half_kelly_fraction = st.slider(
        "Kelly fraction", 0.1, 1.0, float(cfg.half_kelly_fraction), 0.1,
        help="0.5 = Half-Kelly (recommended). Lower = more conservative.",
    )
with col_p2:
    cfg.sigma_target = st.slider(
        "Target annual volatility", 0.05, 0.40, float(cfg.sigma_target), 0.01,
        help="Position sizes scale inversely with realised vol relative to this target.",
    )
    cfg.max_social_allocation = st.slider(
        "Max social media allocation (%)", 5.0, 50.0,
        float(cfg.max_social_allocation * 100), 5.0,
    ) / 100
with col_p3:
    cfg.trade_max_spread_bps = st.number_input(
        "Max spread (bps)", 10, 500, int(cfg.trade_max_spread_bps), 10,
    )
    cfg.trade_min_adv_usd = st.number_input(
        "Min ADV for execution ($)", 100_000, 10_000_000,
        int(cfg.trade_min_adv_usd), 100_000, format="%d",
    )

st.divider()

# ═══════════════════════════════════════════════════════════════
# 5. EXIT RULES
# ═══════════════════════════════════════════════════════════════
st.header("5. Exit Rules")
col_e1, col_e2, col_e3 = st.columns(3)

with col_e1:
    cfg.take_profit_pct = st.slider(
        "Take profit (%)", 1.0, 15.0, float(cfg.take_profit_pct * 100), 0.5,
    ) / 100
    cfg.trailing_stop_pct = st.slider(
        "Trailing stop (%)", 2.0, 20.0, float(cfg.trailing_stop_pct * 100), 0.5,
    ) / 100
with col_e2:
    cfg.atr_multiplier = st.slider(
        "ATR stop multiplier", 0.5, 5.0, float(cfg.atr_multiplier), 0.25,
    )
    cfg.max_hold_hours = st.number_input(
        "Max hold time (hours)", 4, 120, int(cfg.max_hold_hours), 4,
    )
with col_e3:
    cfg.signal_reversal_threshold = st.slider(
        "Sentiment reversal threshold", -0.8, -0.05,
        float(cfg.signal_reversal_threshold), 0.05,
        help="Exit when sentiment score crosses this level.",
    )
    cfg.mention_decay_threshold = st.slider(
        "Mention decay exit (fraction of peak)", 0.05, 0.5,
        float(cfg.mention_decay_threshold), 0.05,
        help="Exit when mentions fall below this fraction of peak.",
    )

st.divider()

# ═══════════════════════════════════════════════════════════════
# 6. RISK & CIRCUIT BREAKERS
# ═══════════════════════════════════════════════════════════════
st.header("6. Risk & Circuit Breakers")
col_r1, col_r2 = st.columns(2)

with col_r1:
    st.subheader("Loss Limits")
    cfg.loss_limit_single_trade = st.slider(
        "Single trade loss limit (%) — emergency exit", 0.5, 5.0,
        float(cfg.loss_limit_single_trade * 100), 0.25,
    ) / 100
    cfg.loss_limit_daily = st.slider(
        "Daily loss limit (%) — halt new trades", 1.0, 10.0,
        float(cfg.loss_limit_daily * 100), 0.5,
    ) / 100
    cfg.loss_limit_weekly = st.slider(
        "Weekly loss limit (%) — reduce sizes 50%", 2.0, 20.0,
        float(cfg.loss_limit_weekly * 100), 0.5,
    ) / 100
    cfg.drawdown_halt = st.slider(
        "Max drawdown (%) — full halt", 5.0, 40.0,
        float(cfg.drawdown_halt * 100), 1.0,
    ) / 100

with col_r2:
    st.subheader("VIX Regime Thresholds")
    st.caption("Position size scalars: 0% / 25% / 50% / 75% / 100% as VIX crosses each level.")
    cfg.vix_crisis            = float(st.number_input("VIX crisis → 0% size",    20, 80, int(cfg.vix_crisis)))
    cfg.vix_high_fear         = float(st.number_input("VIX high fear → 25%",     15, 60, int(cfg.vix_high_fear)))
    cfg.vix_elevated          = float(st.number_input("VIX elevated → 50%",      10, 50, int(cfg.vix_elevated)))
    cfg.vix_slightly_elevated = float(st.number_input("VIX slightly elevated → 75%", 10, 40, int(cfg.vix_slightly_elevated)))

st.divider()

# ═══════════════════════════════════════════════════════════════
# SAVE / RESET
# ═══════════════════════════════════════════════════════════════
col_save, col_reset = st.columns([3, 1])

with col_save:
    if st.button("Save Configuration", type="primary", use_container_width=True):
        errors = save_config(cfg)
        if errors:
            for e in errors:
                st.error(e)
        else:
            st.success("Configuration saved. All services will pick up changes within ~1 minute.")
            st.balloons()

with col_reset:
    if st.button("Reset to Defaults", use_container_width=True):
        errs = save_config(SystemConfig())
        if not errs:
            st.warning("Reset to factory defaults.")
            st.rerun()
