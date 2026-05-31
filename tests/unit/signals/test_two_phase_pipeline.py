"""
Unit tests for two-phase signal pipeline.

Tests cover:
1. DataSourceRegistry.tier1_sources() and tier2_sources() filtering
2. BaseDataSource.tier default = 1
3. TwitterDataSource.tier override = 2
4. signal_service helpers: _stats_has_tier2_data, _is_open_position
5. SystemConfig two-phase config fields
6. signal_service _request_enrichment deduplication
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from social_trading.config.system_config import SystemConfig
from social_trading.ingest.base import BaseDataSource
from social_trading.ingest.registry import DataSourceRegistry
from social_trading.services.signal_service import (
    _TIER2_SOURCE_NAMES,
    _stats_has_tier2_data,
    _request_enrichment,
    _is_open_position,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

class _FakeFreeSrc(BaseDataSource):
    """Tier-1 free source stub."""
    def __init__(self):
        pass  # skip redis/cfg init

    @property
    def name(self) -> str:
        return "free_source"

    @property
    def is_streaming(self) -> bool:
        return False

    async def stream(self):
        return
        yield

    async def poll(self, tickers):
        return []

    async def get_trending(self):
        return []


class _FakePaidSrc(BaseDataSource):
    """Tier-2 paid source stub."""
    def __init__(self):
        pass  # skip redis/cfg init

    @property
    def name(self) -> str:
        return "paid_source"

    @property
    def tier(self) -> int:
        return 2

    @property
    def is_streaming(self) -> bool:
        return False

    async def stream(self):
        return
        yield

    async def poll(self, tickers):
        return []

    async def get_trending(self):
        return []


# ── Tier property tests ───────────────────────────────────────────────────────

class TestSourceTier:
    def test_free_source_defaults_tier_1(self):
        src = _FakeFreeSrc()
        assert src.tier == 1

    def test_paid_source_returns_tier_2(self):
        src = _FakePaidSrc()
        assert src.tier == 2

    def test_twitter_source_tier_is_2(self):
        """TwitterDataSource must override tier=2."""
        from social_trading.ingest.sources.twitter import TwitterDataSource
        # Instantiate with minimal mocks
        fake_redis = MagicMock()
        fake_cfg = MagicMock(spec=SystemConfig)
        src = TwitterDataSource(redis=fake_redis, cfg=fake_cfg, bearer_token="fake")
        assert src.tier == 2

    def test_bluesky_source_tier_is_1(self):
        """BlueskyDataSource should remain Tier-1."""
        from social_trading.ingest.sources.bluesky import BlueskyDataSource
        fake_redis = MagicMock()
        fake_cfg = MagicMock(spec=SystemConfig)
        fake_watchlist = MagicMock()
        src = BlueskyDataSource(
            redis=fake_redis, cfg=fake_cfg,
            watchlist=fake_watchlist,
            handle="test.bsky.social",
            app_password="fake",
        )
        assert src.tier == 1


# ── Registry tier filtering ───────────────────────────────────────────────────

class TestRegistryTierFiltering:
    def _make_registry(self):
        reg = DataSourceRegistry()
        reg.register(_FakeFreeSrc())
        reg.register(_FakePaidSrc())
        return reg

    def test_tier1_sources_returns_only_free(self):
        reg = self._make_registry()
        t1 = reg.tier1_sources()
        assert all(s.tier == 1 for s in t1)
        assert any(s.name == "free_source" for s in t1)
        assert not any(s.name == "paid_source" for s in t1)

    def test_tier2_sources_returns_only_paid(self):
        reg = self._make_registry()
        t2 = reg.tier2_sources()
        assert all(s.tier == 2 for s in t2)
        assert any(s.name == "paid_source" for s in t2)
        assert not any(s.name == "free_source" for s in t2)

    def test_tier1_empty_when_only_paid(self):
        reg = DataSourceRegistry()
        reg.register(_FakePaidSrc())
        assert reg.tier1_sources() == []

    def test_tier2_empty_when_only_free(self):
        reg = DataSourceRegistry()
        reg.register(_FakeFreeSrc())
        assert reg.tier2_sources() == []


# ── _stats_has_tier2_data ─────────────────────────────────────────────────────

class TestStatsHasTier2Data:
    def test_no_tier2_sources_returns_false(self):
        assert _stats_has_tier2_data({"bluesky", "stocktwits"}) is False

    def test_twitter_in_sources_returns_true(self):
        assert _stats_has_tier2_data({"bluesky", "twitter"}) is True

    def test_twitter_only_returns_true(self):
        assert _stats_has_tier2_data({"twitter"}) is True

    def test_empty_sources_returns_false(self):
        assert _stats_has_tier2_data(set()) is False

    def test_tier2_source_names_constant_contains_twitter(self):
        assert "twitter" in _TIER2_SOURCE_NAMES


# ── _is_open_position ─────────────────────────────────────────────────────────

class TestIsOpenPosition:
    @pytest.mark.asyncio
    async def test_returns_true_when_position_exists(self):
        mock_redis = AsyncMock()
        mock_redis.hexists = AsyncMock(return_value=1)
        result = await _is_open_position(mock_redis, "AAPL")
        assert result is True
        mock_redis.hexists.assert_called_once_with("positions:live", "AAPL")

    @pytest.mark.asyncio
    async def test_returns_false_when_no_position(self):
        mock_redis = AsyncMock()
        mock_redis.hexists = AsyncMock(return_value=0)
        result = await _is_open_position(mock_redis, "TSLA")
        assert result is False


# ── _request_enrichment deduplication ────────────────────────────────────────

class TestRequestEnrichment:
    @pytest.mark.asyncio
    async def test_publishes_on_first_call(self):
        mock_redis = AsyncMock()
        mock_redis.set = AsyncMock(return_value=True)  # nx=True: key set successfully
        mock_redis.xadd = AsyncMock()

        await _request_enrichment(mock_redis, "NVDA", 0.45, 60)

        mock_redis.xadd.assert_called_once()
        call_kwargs = mock_redis.xadd.call_args
        event = call_kwargs[0][1]
        assert event["ticker"] == "NVDA"
        assert event["phase1_score"] == "0.45"
        assert "requested_at" in event

    @pytest.mark.asyncio
    async def test_skips_when_already_sent_this_cycle(self):
        mock_redis = AsyncMock()
        # Redis SET NX returns None when key already exists
        mock_redis.set = AsyncMock(return_value=None)
        mock_redis.xadd = AsyncMock()

        await _request_enrichment(mock_redis, "NVDA", 0.45, 60)

        mock_redis.xadd.assert_not_called()

    @pytest.mark.asyncio
    async def test_dedup_key_uses_correct_ttl(self):
        mock_redis = AsyncMock()
        mock_redis.set = AsyncMock(return_value=True)
        mock_redis.xadd = AsyncMock()

        await _request_enrichment(mock_redis, "AAPL", 0.50, 120)

        mock_redis.set.assert_called_once()
        call_args = mock_redis.set.call_args
        assert call_args[1]["ex"] == 120
        assert call_args[1]["nx"] is True
        assert "AAPL" in call_args[0][0]  # key contains ticker


# ── SystemConfig phase fields ─────────────────────────────────────────────────

class TestSystemConfigPhaseFields:
    def test_phase_fields_have_correct_defaults(self):
        cfg = SystemConfig()
        assert cfg.signal_phase1_threshold == pytest.approx(0.40)
        assert cfg.signal_phase2_threshold == pytest.approx(0.65)
        assert cfg.phase2_max_tickers_per_cycle == 10
        assert cfg.phase2_skip_open_positions is True

    def test_phase1_threshold_below_phase2(self):
        """Enforce that the design intent is reflected in defaults."""
        cfg = SystemConfig()
        assert cfg.signal_phase1_threshold < cfg.signal_phase2_threshold
