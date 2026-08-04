from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import app.main as main_module
import pytest
from fastapi import FastAPI


def patch_common_dependencies(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[MagicMock, AsyncMock, AsyncMock]:
    settings = SimpleNamespace(
        redis_url="redis://redis:6379/0",
        analytics_summary_cache_ttl_seconds=60,
        kafka_dlq_topic="payments.dlq",
        kafka_bootstrap_servers="kafka:9092",
        kafka_topic="payments",
        kafka_consumer_group="payflow-analytics-service",
        kafka_auto_offset_reset="earliest",
        app_host="0.0.0.0",
        app_port=8002,
    )

    redis_client = MagicMock()
    redis_client.aclose = AsyncMock()

    check_db_connection = AsyncMock()
    close_db = AsyncMock()

    monkeypatch.setattr(main_module, "configure_logging", lambda: None)
    monkeypatch.setattr(main_module, "Settings", lambda: settings)
    monkeypatch.setattr(main_module, "init_db", lambda settings: None)
    monkeypatch.setattr(
        main_module,
        "get_session_factory",
        lambda: MagicMock(),
    )
    monkeypatch.setattr(
        main_module,
        "check_db_connection",
        check_db_connection,
    )
    monkeypatch.setattr(
        main_module,
        "close_db",
        close_db,
    )
    monkeypatch.setattr(
        main_module.RedisClientAdapter,
        "from_url",
        lambda url: redis_client,
    )

    return redis_client, check_db_connection, close_db


@pytest.mark.asyncio
async def test_lifespan_closes_publisher_when_publisher_startup_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis_client, check_db_connection, close_db = patch_common_dependencies(monkeypatch)

    dead_letter_publisher = MagicMock()
    dead_letter_publisher.start = AsyncMock(
        side_effect=RuntimeError("Kafka unavailable")
    )
    dead_letter_publisher.stop = AsyncMock()

    consumer = MagicMock()
    consumer.start = AsyncMock()
    consumer.stop = AsyncMock()

    dead_letter_publisher_factory = MagicMock(return_value=dead_letter_publisher)
    consumer_factory = MagicMock(return_value=consumer)

    monkeypatch.setattr(
        main_module,
        "DeadLetterPublisher",
        dead_letter_publisher_factory,
    )
    monkeypatch.setattr(
        main_module,
        "PaymentEventConsumer",
        consumer_factory,
    )

    application = FastAPI()

    with pytest.raises(RuntimeError, match="Kafka unavailable"):
        async with main_module.lifespan(application):
            pass

    check_db_connection.assert_awaited_once()

    dead_letter_publisher.start.assert_awaited_once()
    dead_letter_publisher.stop.assert_awaited_once()

    consumer_factory.assert_not_called()
    consumer.start.assert_not_awaited()
    consumer.stop.assert_not_awaited()

    redis_client.aclose.assert_awaited_once()
    close_db.assert_awaited_once()


@pytest.mark.asyncio
async def test_lifespan_closes_kafka_resources_when_consumer_startup_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis_client, check_db_connection, close_db = patch_common_dependencies(monkeypatch)

    dead_letter_publisher = MagicMock()
    dead_letter_publisher.start = AsyncMock()
    dead_letter_publisher.stop = AsyncMock()

    consumer = MagicMock()
    consumer.start = AsyncMock(side_effect=RuntimeError("Kafka consumer unavailable"))
    consumer.stop = AsyncMock()

    monkeypatch.setattr(
        main_module,
        "DeadLetterPublisher",
        MagicMock(return_value=dead_letter_publisher),
    )
    monkeypatch.setattr(
        main_module,
        "PaymentEventConsumer",
        MagicMock(return_value=consumer),
    )

    application = FastAPI()

    with pytest.raises(RuntimeError, match="Kafka consumer unavailable"):
        async with main_module.lifespan(application):
            pass

    check_db_connection.assert_awaited_once()

    dead_letter_publisher.start.assert_awaited_once()
    dead_letter_publisher.stop.assert_awaited_once()

    consumer.start.assert_awaited_once()
    consumer.stop.assert_awaited_once()

    redis_client.aclose.assert_awaited_once()
    close_db.assert_awaited_once()
