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
        "mode":            r.get("trading:mode") or "paper",
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


def pin_ticker(ticker: str) -> None:
    r = _get_redis()
    r.zadd("watchlist:active", {ticker.upper(): time.time()})


def unpin_ticker(ticker: str) -> None:
    r = _get_redis()
    r.zrem("watchlist:active", ticker.upper())


def get_recent_signals_from_redis() -> list[dict]:
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
