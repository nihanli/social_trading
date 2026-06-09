"""
RedisLogHandler — a stdlib logging.Handler that fans log records into a
Redis stream (``logs:{service}``) alongside the normal honcho/stdout output.

Design notes
------------
* Additive only: does NOT remove or replace the existing StreamHandler that
  ``basicConfig`` installs.  Honcho stdout output is completely unchanged.
* Per-service enable/disable flag: ``logs:enabled:{service}`` (Redis string,
  "1" = on, "0" = off).  Checked at most once every FLAG_CHECK_INTERVAL_SEC
  to avoid a Redis GET on every log line.
* Stream bounds: MAXLEN ~500 (soft-trim, O(1)).  TTL of 600 s is reset on
  every write so the stream auto-expires 10 minutes after the last log entry.
* Fault-tolerant: any Redis error is silently swallowed so a Redis hiccup
  never crashes a service.
* Uses the synchronous ``redis-py`` client because ``logging.Handler.emit``
  is a sync method.
"""

from __future__ import annotations

import logging
import time
import traceback
from typing import Any

_STREAM_MAXLEN = 500
_STREAM_TTL_SEC = 600       # stream expires 10 min after last write
_FLAG_CHECK_INTERVAL_SEC = 5  # re-read the enable flag at most every 5 s


class RedisLogHandler(logging.Handler):
    """Publish log records to ``logs:{service}`` Redis stream."""

    def __init__(self, service: str, redis_url: str, level: int = logging.DEBUG) -> None:
        super().__init__(level)
        self._service = service
        self._redis_url = redis_url
        self._stream_key = f"logs:{service}"
        self._flag_key = f"logs:enabled:{service}"

        # Lazy-connect: don't fail __init__ if Redis is not yet up.
        self._redis: Any = None

        # Cached enable state to avoid per-line Redis GETs.
        self._enabled: bool = True
        self._flag_last_checked: float = 0.0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_redis(self) -> Any:
        """Return (and lazily create) a sync Redis connection."""
        if self._redis is None:
            import redis  # noqa: PLC0415
            self._redis = redis.from_url(self._redis_url, socket_connect_timeout=1)
        return self._redis

    def _check_enabled(self) -> bool:
        """Return True if logging to Redis is enabled for this service."""
        now = time.monotonic()
        if now - self._flag_last_checked < _FLAG_CHECK_INTERVAL_SEC:
            return self._enabled
        try:
            r = self._get_redis()
            val = r.get(self._flag_key)
            # Default to enabled when the key is absent (first run).
            self._enabled = (val is None) or (val not in (b"0", "0"))
        except Exception:
            pass  # keep previous cached value on Redis error
        self._flag_last_checked = now
        return self._enabled

    # ------------------------------------------------------------------
    # logging.Handler interface
    # ------------------------------------------------------------------

    def emit(self, record: logging.LogRecord) -> None:
        try:
            if not self._check_enabled():
                return

            # Format exception/traceback as a single line (capped at 500 chars).
            exc_text = ""
            if record.exc_info:
                exc_text = "".join(traceback.format_exception(*record.exc_info))[:500]

            r = self._get_redis()
            r.xadd(
                self._stream_key,
                {
                    "ts":      str(int(record.created * 1000)),  # ms epoch
                    "level":   record.levelname,
                    "logger":  record.name,
                    "msg":     record.getMessage()[:1000],
                    "exc":     exc_text,
                },
                maxlen=_STREAM_MAXLEN,
                approximate=True,
            )
            r.expire(self._stream_key, _STREAM_TTL_SEC)
        except Exception:
            # Never let a logging error crash the service.
            self.handleError(record)
