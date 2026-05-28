"""
sidebar_refresh_countdown — live countdown timer for the Streamlit sidebar.

Reads `discovery_poll_interval_sec` from SystemConfig and `discovery:last_poll_ts`
from Redis to calculate the *actual* time remaining until the next discovery poll.
Renders two progress bars (discovery + sentiment) via st.iframe with a data URI.

Usage (call once per page, before other sidebar content):
    from social_trading.monitoring.streamlit.utils.refresh_countdown import (
        sidebar_refresh_countdown,
    )
    sidebar_refresh_countdown()
"""
from __future__ import annotations

import os
import time
import urllib.parse

import redis as _redis
import streamlit as st

from social_trading.monitoring.streamlit.utils.redis_ctrl import load_config

_DEFAULT_INTERVAL = 300  # fallback if Redis is unavailable


@st.cache_resource
def _get_redis() -> _redis.Redis:
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return _redis.from_url(url, decode_responses=True)


def sidebar_refresh_countdown() -> None:
    """
    Render two live countdown progress bars at the top of the sidebar.

    Uses ``discovery:last_poll_ts`` / ``sentiment:last_poll_ts`` (Unix timestamps
    written by ingest_service after each cycle) to compute true remaining seconds.
    Rendered via st.iframe with a data URI so JavaScript runs in the browser.
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
    sent_pct  = sent_remaining / max(sentiment_interval, 1) * 100

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<style>
  body {{margin:0;padding:4px 2px;font-family:'Segoe UI',Arial,sans-serif;background:transparent;}}
  .label {{font-size:0.65rem;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:0.09em;margin-bottom:5px;}}
  .track {{background:#1a1a2e;border-radius:6px;height:14px;overflow:hidden;border:1px solid #2a2a3e;margin-bottom:10px;}}
  .bar   {{height:14px;border-radius:6px;transition:width 0.95s linear;}}
</style></head>
<body>
  <div class="label">&#9201; Next Discovery Poll</div>
  <div class="track"><div id="disc-bar" class="bar" style="background:linear-gradient(90deg,#00d4ff,#0066cc);width:{disc_pct:.1f}%"></div></div>
  <div class="label">&#128172; Next Sentiment Poll</div>
  <div class="track"><div id="sent-bar" class="bar" style="background:linear-gradient(90deg,#a855f7,#6d28d9);width:{sent_pct:.1f}%"></div></div>
<script>
(function(){{
  var discTotal={discovery_interval},discLeft={disc_remaining};
  var sentTotal={sentiment_interval},sentLeft={sent_remaining};
  var dBar=document.getElementById('disc-bar');
  var sBar=document.getElementById('sent-bar');
  function color(p,a,b,c){{return p>0.5?a:p>0.2?b:c;}}
  function tick(){{
    discLeft=Math.max(0,discLeft-1);
    sentLeft=Math.max(0,sentLeft-1);
    var dp=discLeft/discTotal;
    dBar.style.width=(dp*100)+'%';
    dBar.style.background=color(dp,'linear-gradient(90deg,#00d4ff,#0066cc)','linear-gradient(90deg,#00ffaa,#00aa66)','linear-gradient(90deg,#ff4444,#cc0000)');
    var sp=sentLeft/sentTotal;
    sBar.style.width=(sp*100)+'%';
    sBar.style.background=color(sp,'linear-gradient(90deg,#a855f7,#6d28d9)','linear-gradient(90deg,#f59e0b,#b45309)','linear-gradient(90deg,#ff4444,#cc0000)');
    if(discLeft>0||sentLeft>0)setTimeout(tick,1000);
  }}
  setTimeout(tick,1000);
}})();
</script>
</body></html>"""

    src = "data:text/html;charset=utf-8," + urllib.parse.quote(html)

    with st.sidebar:
        st.iframe(src, height=110)
        st.markdown("---")
