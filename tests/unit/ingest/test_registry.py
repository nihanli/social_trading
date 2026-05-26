"""Unit tests for DataSourceRegistry."""
from __future__ import annotations

import pytest

from social_trading.ingest.registry import DataSourceRegistry


# ── Minimal fake source that satisfies DataSource protocol ───────────────────

class FakeSource:
    name = "fake"
    is_streaming = False

    async def stream(self):
        return
        yield  # pragma: no cover

    async def poll(self, tickers):
        return []

    async def get_trending(self):
        return []

    async def health_check(self):
        return True


class FakeStreamingSource(FakeSource):
    name = "fake_streaming"
    is_streaming = True


class BadSource:
    """Does NOT satisfy DataSource protocol — missing required methods."""
    name = "bad"


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_register_valid_source():
    registry = DataSourceRegistry()
    registry.register(FakeSource())
    assert "fake" in registry.names


def test_register_bad_source_raises():
    registry = DataSourceRegistry()
    with pytest.raises(TypeError, match="DataSource protocol"):
        registry.register(BadSource())  # type: ignore[arg-type]


def test_unregister_removes_source():
    registry = DataSourceRegistry()
    registry.register(FakeSource())
    registry.unregister("fake")
    assert "fake" not in registry.names


def test_unregister_unknown_is_silent():
    registry = DataSourceRegistry()
    registry.unregister("nonexistent")  # should not raise


def test_get_returns_source():
    registry = DataSourceRegistry()
    src = FakeSource()
    registry.register(src)
    assert registry.get("fake") is src


def test_get_returns_none_for_unknown():
    registry = DataSourceRegistry()
    assert registry.get("missing") is None


def test_active_sources_returns_all():
    registry = DataSourceRegistry()
    registry.register(FakeSource())
    registry.register(FakeStreamingSource())
    assert len(registry.active_sources()) == 2


def test_streaming_sources_filter():
    registry = DataSourceRegistry()
    registry.register(FakeSource())
    registry.register(FakeStreamingSource())
    streaming = registry.streaming_sources()
    assert len(streaming) == 1
    assert streaming[0].name == "fake_streaming"


def test_polling_sources_filter():
    registry = DataSourceRegistry()
    registry.register(FakeSource())
    registry.register(FakeStreamingSource())
    polling = registry.polling_sources()
    assert len(polling) == 1
    assert polling[0].name == "fake"


def test_len():
    registry = DataSourceRegistry()
    assert len(registry) == 0
    registry.register(FakeSource())
    assert len(registry) == 1


def test_duplicate_registration_overwrites():
    registry = DataSourceRegistry()
    s1 = FakeSource()
    s2 = FakeSource()
    registry.register(s1)
    registry.register(s2)
    assert len(registry) == 1
    assert registry.get("fake") is s2
