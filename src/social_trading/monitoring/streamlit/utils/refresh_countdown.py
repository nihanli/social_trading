"""
sidebar_refresh_countdown — live countdown timer for the Streamlit sidebar.

Reads `discovery_poll_interval_sec` from SystemConfig and `discovery:last_poll_ts`
from Redis to calculate the *actual* time remaining until the next discovery poll.
Injects JavaScript that counts down from that remaining time, updating a progress
bar and label every second.

Usage (call once per page, before other sidebar content):
    from social_trading.monitoring.streamlit.utils.refresh_countdown import (
        sidebar_refresh_countdown,
    )
    sidebar_refresh_countdown()
"""
from __future__ import annotations

import os
import time

import redis
import streamlit as st
import streamlit.components.v1 as components

from social_trading.monitoring.streamlit.utils.redis_ctrl import load_config

_DEFAULT_INTERVAL = 300  # fallback if Redis is unavailable


@st.cache_resource
def _get_redis() -> redis.Redis:
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return redis.from_url(url, decode_responses=True)


def sidebar_refresh_countdown() -> None:
    """
    Render a live countdown + progress bar at the top of the sidebar.

    Uses ``discovery:last_poll_ts`` (Unix timestamp written by ingest_service
    after each discovery cycle) and ``SystemConfig.discovery_poll_interval_sec``
    to compute the true seconds remaining until the next poll.  The JS counter
    starts from that pre-computed value so it stays accurate across page loads.
    """
    try:
        cfg = load_config()
        discovery_interval = int(cfg.discovery_poll_interval_sec)
        sentiment_interval = int(cfg.stocktwits_poll_interval_sec)
    except Exception:
        discovery_interval = _DEFAULT_INTERVAL
        sentiment_interval = _DEFAULT_INTERVAL

    def _remaining(key: str, interval: int) -> int:
        try:
            r = _get_redis()
            raw = r.get(key)
            if raw is not None:
                elapsed = time.time() - float(raw)
                return max(0, int(interval - elapsed))
        except Exception:
            pass
        return interval

    disc_remaining = _remaining("discovery:last_poll_ts", discovery_interval)
    sent_remaining = _remaining("sentiment:last_poll_ts", sentiment_interval)

    disc_pct = disc_remaining / max(discovery_interval, 1) * 100
    sent_pct = sent_remaining / max(sentiment_interval, 1) * 100

    with st.sidebar:
        components.html(
            f"""
            <div style="font-family:'Segoe UI',Arial,sans-serif; padding: 2px 0 4px 0;">

              <!-- Discovery bar -->
              <div style="
                font-size:0.65rem; font-weight:700; color:#888;
                text-transform:uppercase; letter-spacing:0.09em; margin-bottom:5px;
              ">⏱ Next Discovery Poll</div>
              <div style="
                background:#1a1a2e; border-radius:6px; height:14px;
                overflow:hidden; border:1px solid #2a2a3e; margin-bottom:10px;
              ">
                <div id="disc-bar" style="
                  background:linear-gradient(90deg,#00d4ff,#0066cc);
                  height:14px; width:{disc_pct:.1f}%;
                  border-radius:6px; transition:width 0.95s linear;
                "></div>
              </div>

              <!-- Sentiment bar -->
              <div style="
                font-size:0.65rem; font-weight:700; color:#888;
                text-transform:uppercase; letter-spacing:0.09em; margin-bottom:5px;
              ">💬 Next Sentiment Poll</div>
              <div style="
                background:#1a1a2e; border-radius:6px; height:14px;
                overflow:hidden; border:1px solid #2a2a3e;
              ">
                <div id="sent-bar" style="
                  background:linear-gradient(90deg,#a855f7,#6d28d9);
                  height:14px; width:{sent_pct:.1f}%;
                  border-radius:6px; transition:width 0.95s linear;
                "></div>
              </div>

            </div>
            <script>
              (function() {{
                var discTotal = {discovery_interval};
                var discLeft  = {disc_remaining};
                var sentTotal = {sentiment_interval};
                var sentLeft  = {sent_remaining};
                var discBar   = document.getElementById('disc-bar');
                var sentBar   = document.getElementById('sent-bar');

                function barColor(pct, colors) {{
                  return pct > 0.5 ? colors[0] : pct > 0.2 ? colors[1] : colors[2];
                }}

                function tick() {{
                  discLeft = Math.max(0, discLeft - 1);
                  sentLeft = Math.max(0, sentLeft - 1);

                  var dp = discLeft / discTotal;
                  discBar.style.width = (dp * 100) + '%';
                  discBar.style.background = barColor(dp, [
                    'linear-gradient(90deg,#00d4ff,#0066cc)',
                    'linear-gradient(90deg,#00ffaa,#00aa66)',
                    'linear-gradient(90deg,#ff4444,#cc0000)'
                  ]);

                  var sp = sentLeft / sentTotal;
                  sentBar.style.width = (sp * 100) + '%';
                  sentBar.style.background = barColor(sp, [
                    'linear-gradient(90deg,#a855f7,#6d28d9)',
                    'linear-gradient(90deg,#f59e0b,#b45309)',
                    'linear-gradient(90deg,#ff4444,#cc0000)'
                  ]);

                  if (discLeft > 0 || sentLeft > 0) setTimeout(tick, 1000);
                }}

                setTimeout(tick, 1000);
              }})();
            </script>
            """,
            height=110,
        )
        st.markdown("---")
