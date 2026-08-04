from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

import structlog
from pydantic import ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.schemas.analytics import AnalyticsSummary

logger = structlog.get_logger(__name__)


class RedisClientAdapter:
    def __init__(self, client: Redis) -> None:
        self._client = client

    @classmethod
    def from_url(cls, url: str) -> "RedisClientAdapter":
        client: Redis = Redis.from_url(url)  # pyright: ignore[reportUnknownMemberType]

        return cls(client)

    async def get(self, key: str) -> str | bytes | None:
        return await self._client.get(key)

    async def set(self, key: str, value: str, *, ex: int) -> object:
        return await self._client.set(key, value, ex=ex)

    async def incr(self, key: str) -> int:
        return await self._client.incr(key)

    async def aclose(self) -> None:
        await self._client.aclose()


class RedisClientProtocol(Protocol):
    async def get(
        self,
        key: str,
    ) -> str | bytes | None: ...

    async def set(
        self,
        key: str,
        value: str,
        *,
        ex: int,
    ) -> object: ...

    async def incr(
        self,
        key: str,
    ) -> int: ...


@dataclass(frozen=True, slots=True)
class SummaryCacheLookup:
    key: str | None
    summary: AnalyticsSummary | None


class AnalyticsSummaryCache:
    VERSION_KEY = "analytics:summary:version"
    KEY_PREFIX = "analytics:summary"

    def __init__(
        self,
        redis: RedisClientProtocol,
        ttl_seconds: int,
    ) -> None:
        self._redis = redis
        self._ttl_seconds = ttl_seconds

    async def lookup(
        self,
        *,
        currency: str | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> SummaryCacheLookup:
        try:
            raw_version = await self._redis.get(self.VERSION_KEY)
            version = self._parse_version(raw_version)

            key = self._build_key(
                version=version, currency=currency, date_from=date_from, date_to=date_to
            )

            payload = await self._redis.get(key)
        except (RedisError, ValueError):
            logger.warning("analytics_summary_cache_read_failed", exc_info=True)
            return SummaryCacheLookup(key=None, summary=None)

        if payload is None:
            logger.info("analytics_summary_cache_miss", cache_key=key)
            return SummaryCacheLookup(key=key, summary=None)

        try:
            summary = AnalyticsSummary.model_validate_json(payload)
        except ValidationError:
            logger.warning(
                "analytics_summary_cache_payload_invalid",
                cache_key=key,
                exc_info=True,
            )
            return SummaryCacheLookup(key=key, summary=None)

        logger.info("analytics_summary_cache_hit", cache_key=key)

        return SummaryCacheLookup(key=key, summary=summary)

    async def store(self, *, key: str | None, summary: AnalyticsSummary) -> None:
        if key is None:
            return

        try:
            await self._redis.set(key, summary.model_dump_json(), ex=self._ttl_seconds)
        except RedisError:
            logger.warning(
                "analytics_summary_cache_write_failed",
                cache_key=key,
                exc_info=True,
            )
            return

        logger.info(
            "analytics_summary_cache_stored",
            cache_key=key,
            ttl_seconds=self._ttl_seconds,
        )

    async def invalidate(self) -> None:
        try:
            version = await self._redis.incr(self.VERSION_KEY)
        except RedisError:
            logger.warning(
                "analytics_summary_cache_invalidation_failed",
                exc_info=True,
            )
            return

        logger.info("analytics_summary_cache_invalidated", cache_version=version)

    @staticmethod
    def _parse_version(raw_version: str | bytes | None) -> int:
        if raw_version is None:
            return 0

        if isinstance(raw_version, bytes):
            raw_version = raw_version.decode("ascii")

        return int(raw_version)

    @classmethod
    def _build_key(
        cls,
        *,
        version: int,
        currency: str | None,
        date_from: datetime | None,
        date_to: datetime | None,
    ) -> str:
        currency_part = currency or "all"
        date_from_part = date_from.isoformat() if date_from is not None else "all"
        date_to_part = date_to.isoformat() if date_to is not None else "all"

        return (
            f"{cls.KEY_PREFIX}:v{version}"
            f":currency={currency_part}"
            f":from={date_from_part}"
            f":to={date_to_part}"
        )
