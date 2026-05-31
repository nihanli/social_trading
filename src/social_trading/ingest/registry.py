"""
DataSourceRegistry — plugin system for social media data sources.

To add a new data source:
  1. Implement BaseDataSource (ingest/base.py)
  2. Call registry.register(YourDataSource(redis, cfg))

The registry verifies the DataSource protocol at registration time so
misconfigured sources fail fast at startup, not silently at runtime.
"""
from __future__ import annotations

import logging

from social_trading.core.protocols import DataSource

logger = logging.getLogger(__name__)


class DataSourceRegistry:
    """
    Central registry for pluggable data sources.

    Usage:
        registry = DataSourceRegistry()
        registry.register(TwitterDataSource(redis, cfg))
        registry.register(RedditDataSource(reddit_client, redis, cfg))

        for source in registry.active_sources():
            ...
    """

    def __init__(self) -> None:
        self._sources: dict[str, DataSource] = {}

    def register(self, source: DataSource) -> None:
        """
        Register a data source.
        Raises TypeError if the source does not satisfy the DataSource protocol.
        """
        if not isinstance(source, DataSource):
            raise TypeError(
                f"{source!r} does not satisfy the DataSource protocol. "
                "Implement name, is_streaming, stream(), poll(), get_trending(), health_check()."
            )
        self._sources[source.name] = source
        logger.info("Registered data source: %s (streaming=%s)", source.name, source.is_streaming)

    def unregister(self, name: str) -> None:
        """Remove a source by name. Silent if not registered."""
        if name in self._sources:
            del self._sources[name]
            logger.info("Unregistered data source: %s", name)

    def get(self, name: str) -> DataSource | None:
        """Look up a source by name."""
        return self._sources.get(name)

    def active_sources(self) -> list[DataSource]:
        """Return all registered sources."""
        return list(self._sources.values())

    def streaming_sources(self) -> list[DataSource]:
        """Return only sources where is_streaming=True."""
        return [s for s in self._sources.values() if s.is_streaming]

    def polling_sources(self) -> list[DataSource]:
        """Return only sources where is_streaming=False."""
        return [s for s in self._sources.values() if not s.is_streaming]

    def tier1_sources(self) -> list[DataSource]:
        """Return Tier-1 (free/always-on) polling sources."""
        return [s for s in self._sources.values() if getattr(s, "tier", 1) == 1]

    def tier2_sources(self) -> list[DataSource]:
        """Return Tier-2 (metered/paid) sources used only for Phase-2 enrichment."""
        return [s for s in self._sources.values() if getattr(s, "tier", 1) == 2]

    @property
    def names(self) -> list[str]:
        """Names of all registered sources."""
        return list(self._sources.keys())

    def __len__(self) -> int:
        return len(self._sources)

    def __repr__(self) -> str:
        return f"DataSourceRegistry({self.names})"
