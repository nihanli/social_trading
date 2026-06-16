"""
Redis control helpers for the Streamlit monitoring UI.

Two responsibilities:
  1. Read system state (circuit breaker, drawdown, VIX, mode) from Redis.
  2. Send control commands to the execution engine via Redis pub/sub channel
     "trading:commands".

The execution service listens on "trading:commands" and honours:
  HALT_NEW      — stop opening new positions
  RESUME        — re-enable new positions
  CLOSE_ALL     — emergency: close every open position
  CLOSE_TICKER  — close one ticker (payload: {"ticker": "AAPL"})
  CONFIG_UPDATED — hint to services to reload config next cycle

Config load/save is delegated to SystemConfig (async-aware wrapper provided
here using a tiny sync shim so Streamlit pages don't need to run asyncio).
"""
from __future__ import annotations

import contextlib
import json
import os
import time
from datetime import UTC, datetime

import redis
import streamlit as st

# Import SystemConfig synchronously — Streamlit pages are not async
from social_trading.config.system_config import SystemConfig


@st.cache_resource
def _get_redis() -> redis.Redis:
    """Return a cached synchronous Redis client."""
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return redis.from_url(url, decode_responses=True)


def get_system_state() -> dict:
    """
    Read live system state from Redis hashes / keys.
    Falls back to safe defaults when keys are absent (e.g. before first run).
    """
    r = _get_redis()

    def _fget(key: str, default: float = 0.0) -> float:
        v = r.get(key)
        return float(v) if v is not None else default

    # Circuit breaker state persisted by risk_service
    cb_raw = r.get("circuit:state")
    circuit = "NORMAL"
    if cb_raw:
        with contextlib.suppress(Exception):
            circuit = json.loads(cb_raw).get("state", "NORMAL")

    # Account state written by execution_service
    acct = r.hgetall("account:state") or {}

    def _af(key: str, default: float = 0.0) -> float:
        v = acct.get(key)
        return float(v) if v is not None else default

    return {
        "circuit":         circuit,
        "halt_new":        r.get("trading:halt_new") == "1",
        "daily_pnl_pct":   _af("daily_pnl") / max(_af("net_liquidation", 100_000), 1) * 100,
        "drawdown":        _af("drawdown_pct"),
        "net_liquidation": _af("net_liquidation", 100_000),
        "vix":             float(r.get("market:vix") or 0.0),
        "mode":            r.get("trading:mode") or "live",
        # "1" = connected, "0" = disconnected, None = service not running
        "ib_connected":    r.get("ib:connected"),
        "svc_alive":       r.get("service:heartbeat") is not None,
    }


def send_command(cmd: str, payload: dict | None = None) -> None:
    """Publish a control command to the execution engine."""
    r = _get_redis()
    message = json.dumps({
        "cmd": cmd,
        "payload": payload or {},
        "ts": datetime.now(UTC).isoformat(),
    })
    r.publish("trading:commands", message)


def halt_new_trades() -> None:
    r = _get_redis()
    r.set("trading:halt_new", "1")
    send_command("HALT_NEW")


def resume_trading() -> None:
    r = _get_redis()
    r.delete("trading:halt_new")
    send_command("RESUME")


def close_all_positions() -> None:
    send_command("CLOSE_ALL")


def close_position(ticker: str) -> None:
    send_command("CLOSE_TICKER", {"ticker": ticker.upper()})


def trigger_sync_reconcile() -> None:
    """Publish REFRESH_SYNC so the execution service re-runs inflight order recovery."""
    send_command("REFRESH_SYNC")


def get_fill_sync_alerts() -> list[dict]:
    """
    Return all active fill-sync alert entries from alerts:fill_sync.
    Each dict has: ticker, order_id, type, severity, message, age_minutes, updated_at.
    """
    import json as _json
    r = _get_redis()
    result = []
    try:
        raw = r.hgetall("alerts:fill_sync") or {}
        for _, v in raw.items():
            try:
                result.append(_json.loads(v))
            except Exception:
                continue
    except Exception:
        pass
    return sorted(result, key=lambda a: a.get("age_minutes", 0), reverse=True)


def dismiss_fill_sync_alert(order_id: str) -> None:
    """Dismiss a specific fill-sync alert by order_id."""
    r = _get_redis()
    try:
        r.hdel("alerts:fill_sync", order_id)
    except Exception:
        pass


def dismiss_all_fill_sync_alerts() -> None:
    """Dismiss all active fill-sync alerts."""
    r = _get_redis()
    try:
        r.delete("alerts:fill_sync")
    except Exception:
        pass


def get_pending_reconcile() -> list[dict]:
    """
    Return all entries from positions:pending_reconcile.

    These are positions persisted in the app that could not be automatically
    reconciled against IB at startup (no current IB position AND no fill record).
    Each dict includes: ticker, direction, entry_price, shares, opened_at,
    reason, message, last_checked_at.
    """
    r = _get_redis()
    result = []
    try:
        raw = r.hgetall("positions:pending_reconcile") or {}
        for _, v in raw.items():
            try:
                result.append(json.loads(v))
            except Exception:
                continue
    except Exception:
        pass
    return sorted(result, key=lambda x: x.get("opened_at", ""), reverse=True)


def resolve_pending_position(ticker: str, action: str) -> None:
    """
    Ask the execution service to resolve a pending-reconcile position.

    action="close"  → publishes position_closed (exit_price=0), removes from tracking
    action="delete" → removes from app state with no DB event
    """
    if action == "close":
        send_command("RESOLVE_PENDING_CLOSE", {"ticker": ticker.upper()})
    elif action == "delete":
        send_command("RESOLVE_PENDING_DELETE", {"ticker": ticker.upper()})


def get_reconcile_full() -> dict:
    """Return full reconcile snapshot from reconcile:full (written by _reconcile_ib_state)."""
    r = _get_redis()
    try:
        raw = r.get("reconcile:full")
        if raw:
            return json.loads(raw)
    except Exception:
        pass
    return {}


def get_reconcile_last_run() -> str:
    """Return ISO timestamp of last successful reconcile, or empty string."""
    r = _get_redis()
    try:
        v = r.get("reconcile:last_run")
        return v or ""
    except Exception:
        return ""


def get_reconcile_conflicts() -> dict[str, dict]:
    """Return active reconcile conflicts as {ticker: conflict_data}."""
    r = _get_redis()
    result: dict[str, dict] = {}
    try:
        raw = r.hgetall("reconcile:conflicts") or {}
        for k, v in raw.items():
            try:
                result[k] = json.loads(v)
            except Exception:
                result[k] = {"state": "unknown", "reason": str(v)}
    except Exception:
        pass
    return result


def resolve_conflict(ticker: str, action: str) -> None:
    """Send RESOLVE_CONFLICT command: action = mark_closed | remove_app | use_ib_direction."""
    send_command("RESOLVE_CONFLICT", {"ticker": ticker.upper(), "action": action})


def trigger_reconcile_now() -> None:
    """Trigger an immediate reconcile cycle via FULL_RECONCILE command."""
    send_command("FULL_RECONCILE")


# Backward-compatible alias used by older pages
trigger_full_reconcile = trigger_reconcile_now


def get_reconcile_state() -> str:
    """Return current reconcile state: collecting|awaiting_approval|approved|skipped_no_ib|''"""
    r = _get_redis()
    try:
        v = r.get("reconcile:state")
        return v if v else ""
    except Exception:
        return ""


def get_reconcile_data() -> dict:
    """Return full reconcile data dict from reconcile:data, or {}."""
    r = _get_redis()
    try:
        raw = r.get("reconcile:data")
        if raw:
            return json.loads(raw)
    except Exception:
        pass
    return {}


def approve_reconcile() -> None:
    """Send RECONCILE_APPROVE command to execution service."""
    send_command("RECONCILE_APPROVE")


def delete_reconcile_position(ticker: str) -> None:
    """Send RECONCILE_DELETE_POSITION command for the given ticker."""
    send_command("RECONCILE_DELETE_POSITION", {"ticker": ticker.upper()})


def skip_reconcile() -> None:
    """Skip the startup reconcile — sends RECONCILE_SKIP command so the execution
    service also marks the session reconcile as done (preventing a re-run on reconnect)."""
    send_command("RECONCILE_SKIP")


def adopt_ib_position(ticker: str) -> None:
    """Adopt an orphaned IB position into the system (manual_ib → system tracked)."""
    send_command("ADOPT_IB_POSITION", {"ticker": ticker.upper()})


# ── Config helpers ────────────────────────────────────────────────────────────

def _sync_load_config() -> SystemConfig:
    """
    Load SystemConfig from Redis synchronously (used by Streamlit pages).
    Falls back to defaults when Redis is unreachable.
    """
    try:
        import asyncio
        r_async = __import__("redis.asyncio", fromlist=["Redis"])
        url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        loop = asyncio.new_event_loop()
        rc = r_async.from_url(url, decode_responses=False)

        async def _load():
            try:
                return await SystemConfig.load(rc)
            finally:
                await rc.aclose()

        return loop.run_until_complete(_load())
    except Exception:
        return SystemConfig()


def load_config() -> SystemConfig:
    return _sync_load_config()


def save_config(cfg: SystemConfig) -> list[str]:
    """
    Validate then persist config to Redis.
    Returns list of validation errors (empty = success).
    """
    errors = cfg.validate()
    if not errors:
        try:
            import asyncio

            import redis.asyncio as aioredis
            url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
            loop = asyncio.new_event_loop()
            rc = aioredis.from_url(url, decode_responses=False)

            async def _save():
                try:
                    await cfg.save(rc)
                finally:
                    await rc.aclose()

            loop.run_until_complete(_save())
            send_command("CONFIG_UPDATED", {"ts": datetime.now(UTC).isoformat()})
        except Exception as exc:
            errors.append(f"Failed to save to Redis: {exc}")
    return errors


# ── Watchlist helpers ─────────────────────────────────────────────────────────

def get_watchlist() -> list[str]:
    r = _get_redis()
    active = r.zrange("watchlist:active", 0, -1)
    return sorted(active)


def get_pinned_tickers() -> set[str]:
    """Return the set of trader-pinned seed tickers."""
    r = _get_redis()
    return set(r.smembers("watchlist:seed") or [])


def pin_ticker(ticker: str) -> None:
    """
    Pin a ticker so it is always active and never auto-expires.
    Updates both the runtime Redis SET and the durable SystemConfig so pins
    survive service restarts.
    """
    ticker = ticker.upper().strip()
    r = _get_redis()
    import time as _time
    r.sadd("watchlist:seed", ticker)
    r.zadd("watchlist:active", {ticker: _time.time()})

    # Persist to SystemConfig so seed_from_config() re-pins on restart
    cfg = load_config()
    if ticker not in cfg.seed_tickers:
        cfg.seed_tickers = sorted(set(cfg.seed_tickers) | {ticker})
        save_config(cfg)


def unpin_ticker(ticker: str) -> None:
    """
    Remove a ticker from the permanent seed list.
    It will remain in the active watchlist until it expires naturally.
    Updates both the runtime Redis SET and the durable SystemConfig.
    """
    ticker = ticker.upper().strip()
    r = _get_redis()
    r.srem("watchlist:seed", ticker)

    # Persist to SystemConfig so the ticker is not re-pinned on restart
    cfg = load_config()
    if ticker in cfg.seed_tickers:
        cfg.seed_tickers = sorted(set(cfg.seed_tickers) - {ticker})
        save_config(cfg)


def clear_watchlist() -> int:
    """
    Remove all non-pinned tickers from the active watchlist and flush candidates.
    Returns the number of tickers removed.
    """
    r = _get_redis()
    seeds = set(r.smembers("watchlist:seed") or [])
    active = r.zrange("watchlist:active", 0, -1)
    to_remove = [t for t in active if t not in seeds]
    if to_remove:
        r.zrem("watchlist:active", *to_remove)
    r.delete("watchlist:candidates")
    return len(to_remove)


def get_enrichment_queue_size() -> int:
    """
    Return the number of Phase-1 tickers currently queued for Tier-2 enrichment.
    This is the pending-enrichment backlog — tickers that passed Phase 1 but
    haven't yet been re-evaluated with Tier-2 data.
    """
    r = _get_redis()
    try:
        info = r.xinfo_stream("enrichment:requests")
        return int(info.get("length", 0))
    except Exception:
        return 0


def get_phase_pipeline_stats() -> dict:
    """
    Return live two-phase pipeline stats from Redis.
    Reads the enrichment:requests stream length and the tier-2 active flag.

    ``ingest:tier2_active`` is stamped by the enrichment loop each cycle after
    dynamically registering/deregistering Twitter — it reflects actual ingest
    capability, not just what the config flags say.
    """
    r = _get_redis()
    stats: dict = {"enrichment_queue": 0, "tier2_configured": False}
    try:
        # Prefer the live ingest-service flag over raw config to avoid the
        # race where x_api_enabled=True but Twitter isn't yet registered.
        raw = r.get("ingest:tier2_active")
        if raw is not None:
            stats["tier2_configured"] = raw in ("1", b"1")
        else:
            # Fall back to config if ingest_service hasn't written the key yet.
            import json as _json
            cfg_raw = r.get("config:system")
            if cfg_raw:
                cfg_data = _json.loads(cfg_raw)
                stats["tier2_configured"] = bool(cfg_data.get("x_api_enabled"))
    except Exception:
        pass
    try:
        info = r.xinfo_stream("enrichment:requests")
        stats["enrichment_queue"] = int(info.get("length", 0))
    except Exception:
        pass
    return stats


def get_live_positions() -> list[dict]:
    """
    Read open positions directly from the positions:live Redis hash.
    Returns near-real-time data without the 30-second DB sync lag.
    """
    import json as _json
    r = _get_redis()
    result = []
    try:
        raw = r.hgetall("positions:live")
        for _, v in raw.items():
            try:
                p = _json.loads(v)
                result.append(p)
            except Exception:
                continue
    except Exception:
        pass
    return result


    """
    Read recent signals from Redis stream as fallback when PG not available.
    Returns up to 20 most recent entries from strategy_signals stream.
    """
    r = _get_redis()
    try:
        entries = r.xrevrange("strategy_signals", count=20)
        result = []
        for _msg_id, fields in entries:
            result.append({k: v for k, v in fields.items()})
        return result
    except Exception:
        return []


# ── Source on/off helpers ─────────────────────────────────────────────────────

_SOURCES_REGISTRY_KEY = "ingest:sources:registry"
_SOURCES_ENABLED_KEY = "ingest:sources:enabled"


def get_source_registry() -> dict[str, dict]:
    """
    Return all sources registered by ingest_service at startup.
    Returns ``{name: {"tier": int, "streaming": bool}}``.
    Falls back to an empty dict when ingest_service has not yet written the key.
    """
    r = _get_redis()
    raw = r.hgetall(_SOURCES_REGISTRY_KEY) or {}
    result: dict[str, dict] = {}
    for name, v in raw.items():
        with contextlib.suppress(Exception):
            result[name] = json.loads(v)
    return result


def get_source_enabled_states() -> dict[str, bool]:
    """
    Return the current runtime enabled state for each source.
    A source not present in the hash defaults to ``True`` (enabled).
    """
    r = _get_redis()
    raw = r.hgetall(_SOURCES_ENABLED_KEY) or {}
    return {k: v != "0" for k, v in raw.items()}


def set_source_enabled(name: str, enabled: bool) -> None:
    """
    Enable or disable a source at runtime.
    The change is picked up by ingest_service on the next poll cycle
    (~30 s when disabled, next interval when re-enabled).
    """
    r = _get_redis()
    r.hset(_SOURCES_ENABLED_KEY, name, "1" if enabled else "0")