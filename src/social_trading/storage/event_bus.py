"""
TradingEventBus — thin async wrapper around Redis Streams.

Implements the EventBus protocol (core/protocols.py).
Each service publishes events and consumes them via consumer groups,
ensuring each event is processed exactly once per group.

Stream topology (design §8):
    raw_social        ingest → nlp
    sentiment_signals nlp    → signal
    market_data       market → signal, risk
    strategy_signals  signal → risk
    selected_signals  risk   → execution
"""
from __future__ import annotations

import logging
from typing import Any

import redis.asyncio as aioredis

from social_trading.core.events import STREAM_MAXLEN


class TradingEventBus:
    """Redis Streams event bus satisfying the EventBus protocol."""

    def __init__(self, redis: aioredis.Redis) -> None:
        self._redis = redis

    async def publish(self, stream: str, event: dict[str, Any]) -> str:
        """
        Append event to stream.
        All values are coerced to str (Redis Streams requirement).
        Applies approximate MAXLEN trimming to keep stream size bounded.
        Returns the message ID assigned by Redis.
        """
        str_event = {k: str(v) for k, v in event.items()}
        maxlen = STREAM_MAXLEN.get(stream)
        msg_id: bytes = await self._redis.xadd(  # type: ignore[assignment]
            stream, str_event,
            maxlen=maxlen, approximate=True,
        )
        return msg_id.decode() if isinstance(msg_id, bytes) else msg_id

    async def consume(
        self,
        stream: str,
        group: str,
        consumer: str,
        count: int = 10,
        block_ms: int = 2000,
    ) -> list[tuple[str, dict[str, Any]]]:
        """
        Read up to `count` unprocessed messages from a consumer group.
        Blocks for `block_ms` ms if no messages are available (avoids busy-loop).
        Returns list of (message_id, fields_dict) tuples.
        Fields are decoded from bytes to str automatically.
        """
        raw = await self._redis.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams={stream: ">"},  # ">" = undelivered only
            count=count,
            block=block_ms,
        )
        if not raw:
            return []

        results: list[tuple[str, dict[str, Any]]] = []
        for _stream, messages in raw:
            for msg_id, fields in messages:
                mid = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
                decoded = {
                    (k.decode() if isinstance(k, bytes) else k): (
                        v.decode() if isinstance(v, bytes) else v
                    )
                    for k, v in fields.items()
                }
                results.append((mid, decoded))
        return results

    async def ack(self, stream: str, group: str, message_id: str) -> None:
        """Acknowledge a processed message so it won't be re-delivered."""
        await self._redis.xack(stream, group, message_id)

    async def create_group(self, stream: str, group: str) -> None:
        """
        Create a consumer group starting from the beginning of the stream.
        Safe to call multiple times — ignores BUSYGROUP error (already exists).
        """
        try:
            # MKSTREAM: creates stream if it doesn't exist yet
            await self._redis.xgroup_create(stream, group, id="0", mkstream=True)
            logger.info("Created consumer group '%s' on stream '%s'", group, stream)
        except aioredis.ResponseError as exc:
            if "BUSYGROUP" in str(exc):
                pass  # group already exists — expected on restart
            else:
                raise

    async def stream_length(self, stream: str) -> int:
        """Return number of messages in stream (useful for health checks)."""
        return await self._redis.xlen(stream)  # type: ignore[return-value]
