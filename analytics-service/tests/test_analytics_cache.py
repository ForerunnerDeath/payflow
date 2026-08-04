from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
from app.schemas.analytics import AnalyticsSummary, CurrencySummary
from app.services.analytics_cache import AnalyticsSummaryCache, RedisClientProtocol
from redis.exceptions import RedisError


@pytest.mark.asyncio
async def test_lookup_returns_cache_miss_with_generated_key() -> None:
    redis = MagicMock(spec=RedisClientProtocol)
    redis.get = AsyncMock(side_effect=[b"3", None])

    cache = AnalyticsSummaryCache(redis=redis, ttl_seconds=60)

    date_from = datetime(2026, 8, 1, 0, 0, tzinfo=UTC)
    date_to = datetime(2026, 8, 31, 23, 59, tzinfo=UTC)

    lookup = await cache.lookup(currency="RUB", date_from=date_from, date_to=date_to)

    assert lookup.summary is None
    assert lookup.key == (
        "analytics:summary:v3"
        ":currency=RUB"
        ":from=2026-08-01T00:00:00+00:00"
        ":to=2026-08-31T23:59:00+00:00"
    )

    assert redis.get.await_count == 2
    redis.get.assert_any_await("analytics:summary:version")
    redis.get.assert_any_await(lookup.key)


@pytest.mark.asyncio
async def test_lookup_returns_cached_summary() -> None:
    summary = AnalyticsSummary(
        total_transactions=1,
        completed_transactions=1,
        failed_transactions=0,
        currencies=[
            CurrencySummary(
                currency="RUB",
                transaction_count=1,
                total_amount=Decimal("1500.50"),
                average_amount=Decimal("1500.50"),
            )
        ],
    )

    redis = MagicMock(spec=RedisClientProtocol)
    redis.get = AsyncMock(side_effect=[b"3", summary.model_dump_json()])

    cache = AnalyticsSummaryCache(redis=redis, ttl_seconds=60)

    lookup = await cache.lookup(currency="RUB")

    assert lookup.summary == summary
    assert lookup.key == ("analytics:summary:v3:currency=RUB:from=all:to=all")

    assert redis.get.await_count == 2


@pytest.mark.asyncio
async def test_store_saves_summary_with_ttl() -> None:
    redis = MagicMock(spec=RedisClientProtocol)
    redis.set = AsyncMock(return_value=True)

    cache = AnalyticsSummaryCache(redis=redis, ttl_seconds=60)

    summary = AnalyticsSummary(
        total_transactions=1,
        completed_transactions=1,
        failed_transactions=0,
        currencies=[
            CurrencySummary(
                currency="RUB",
                transaction_count=1,
                total_amount=Decimal("1500.50"),
                average_amount=Decimal("1500.50"),
            )
        ],
    )

    key = "analytics:summary:v3:currency=RUB:from=all:to=all"

    await cache.store(key=key, summary=summary)

    redis.set.assert_awaited_once_with(
        key,
        summary.model_dump_json(),
        ex=60,
    )


@pytest.mark.asyncio
async def test_invalidate_increments_cache_version() -> None:
    redis = MagicMock(spec=RedisClientProtocol)
    redis.incr = AsyncMock(return_value=4)

    cache = AnalyticsSummaryCache(redis=redis, ttl_seconds=60)

    await cache.invalidate()

    redis.incr.assert_awaited_once_with("analytics:summary:version")


@pytest.mark.asyncio
async def test_lookup_returns_empty_result_when_redis_is_unavailable() -> None:
    redis = MagicMock(spec=RedisClientProtocol)
    redis.get = AsyncMock(side_effect=RedisError("Redis is unavailable"))

    cache = AnalyticsSummaryCache(redis=redis, ttl_seconds=60)

    lookup = await cache.lookup(currency="RUB")

    assert lookup.summary is None
    assert lookup.key is None

    redis.get.assert_awaited_once_with("analytics:summary:version")


@pytest.mark.asyncio
async def test_lookup_returns_cache_miss_when_payload_is_invalid() -> None:
    redis = MagicMock(spec=RedisClientProtocol)
    redis.get = AsyncMock(side_effect=[b"3", '{"unexpected_field": true}'])

    cache = AnalyticsSummaryCache(redis=redis, ttl_seconds=60)

    lookup = await cache.lookup(currency="RUB")

    assert lookup.summary is None
    assert lookup.key == ("analytics:summary:v3:currency=RUB:from=all:to=all")

    assert redis.get.await_count == 2


@pytest.mark.asyncio
async def test_store_does_not_raise_when_redis_is_unavailable() -> None:
    redis = MagicMock(spec=RedisClientProtocol)
    redis.set = AsyncMock(side_effect=RedisError("Redis is unavailable"))

    cache = AnalyticsSummaryCache(redis=redis, ttl_seconds=60)

    summary = AnalyticsSummary(
        total_transactions=0,
        completed_transactions=0,
        failed_transactions=0,
        currencies=[],
    )

    await cache.store(
        key="analytics:summary:v3:currency=all:from=all:to=all",
        summary=summary,
    )

    redis.set.assert_awaited_once_with(
        "analytics:summary:v3:currency=all:from=all:to=all",
        summary.model_dump_json(),
        ex=60,
    )


@pytest.mark.asyncio
async def test_invalidate_does_not_raise_when_redis_is_unavailable() -> None:
    redis = MagicMock(spec=RedisClientProtocol)
    redis.incr = AsyncMock(side_effect=RedisError("Redis is unavailable"))

    cache = AnalyticsSummaryCache(redis=redis, ttl_seconds=60)

    await cache.invalidate()

    redis.incr.assert_awaited_once_with("analytics:summary:version")
