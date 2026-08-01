import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from app.core.config import Settings
from app.main import app, lifespan


@pytest.mark.asyncio
async def test_lifespan_starts_and_stops_outbox_components() -> None:
    settings = Settings.model_validate(
        {
            "database_url": (
                "postgresql+asyncpg://payflow:payflow@localhost:5433/payflow"
            ),
            "kafka_bootstrap_servers": "localhost:29092",
            "kafka_topic": "payments",
            "outbox_relay_batch_size": 10,
            "outbox_relay_poll_interval_seconds": 1.0,
            "app_host": "127.0.0.1",
            "app_port": 8000,
            "payment_provider_url": "http://127.0.0.1:8001",
            "payment_provider_timeout_seconds": 2.0,
            "payment_provider_max_attempts": 3,
            "payment_provider_retry_base_delay_seconds": 0.5,
            "payment_provider_failure_threshold": 3,
            "payment_provider_recovery_timeout_seconds": 30.0,
        }
    )

    order: list[str] = []

    async def check_db() -> None:
        order.append("database_checked")

    async def start_kafka() -> None:
        order.append("kafka_started")

    async def run_relay(stop_event: asyncio.Event) -> None:
        order.append("relay_started")
        await stop_event.wait()
        order.append("relay_stopped")

    async def stop_kafka() -> None:
        order.append("kafka_stopped")

    async def close_http_client() -> None:
        order.append("http_client_closed")

    async def close_database() -> None:
        order.append("database_closed")

    raw_kafka_producer = MagicMock()

    kafka_producer = MagicMock()
    kafka_producer.start = AsyncMock(side_effect=start_kafka)
    kafka_producer.stop = AsyncMock(side_effect=stop_kafka)

    outbox_relay = MagicMock()
    outbox_relay.run = AsyncMock(side_effect=run_relay)

    http_client = MagicMock()
    http_client.aclose = AsyncMock(side_effect=close_http_client)

    session_factory = MagicMock()
    logger = MagicMock()

    with (
        patch("app.main.Settings", return_value=settings),
        patch("app.main.configure_logging"),
        patch("app.main.init_db") as init_db,
        patch(
            "app.main.check_db_connection", new=AsyncMock(side_effect=check_db)
        ) as check_db_connection,
        patch(
            "app.main.close_db", new=AsyncMock(side_effect=close_database)
        ) as close_db,
        patch(
            "app.main.get_session_factory", return_value=session_factory
        ) as get_session_factory,
        patch("app.main.httpx.AsyncClient", return_value=http_client),
        patch(
            "app.main.AIOKafkaProducer", return_value=raw_kafka_producer
        ) as kafka_producer_class,
        patch(
            "app.main.KafkaEventProducer",
            return_value=kafka_producer,
        ) as event_producer_class,
        patch(
            "app.main.OutboxRelay",
            return_value=outbox_relay,
        ) as relay_class,
        patch(
            "app.main.structlog.get_logger",
            return_value=logger,
        ),
    ):
        async with lifespan(app):
            # Даём созданной через create_task задаче реально запуститься.
            await asyncio.sleep(0)

            assert order == [
                "database_checked",
                "kafka_started",
                "relay_started",
            ]

            kafka_producer.start.assert_awaited_once_with()
            outbox_relay.run.assert_awaited_once()

    assert order == [
        "database_checked",
        "kafka_started",
        "relay_started",
        "relay_stopped",
        "kafka_stopped",
        "http_client_closed",
        "database_closed",
    ]

    init_db.assert_called_once_with(settings)
    check_db_connection.assert_awaited_once_with()
    get_session_factory.assert_called_once_with()

    kafka_producer_class.assert_called_once_with(bootstrap_servers="localhost:29092")
    event_producer_class.assert_called_once_with(
        topic="payments",
        producer=raw_kafka_producer,
    )
    relay_class.assert_called_once_with(
        session_factory=session_factory,
        producer=kafka_producer,
        batch_size=10,
        poll_interval_seconds=1.0,
    )

    kafka_producer.stop.assert_awaited_once_with()
    http_client.aclose.assert_awaited_once_with()
    close_db.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_lifespan_closes_resources_when_relay_fails_on_shutdown() -> None:
    settings = Settings.model_validate(
        {
            "database_url": (
                "postgresql+asyncpg://payflow:payflow@localhost:5433/payflow"
            ),
            "kafka_bootstrap_servers": "localhost:29092",
            "kafka_topic": "payments",
            "outbox_relay_batch_size": 10,
            "outbox_relay_poll_interval_seconds": 1.0,
            "app_host": "127.0.0.1",
            "app_port": 8000,
            "payment_provider_url": "http://127.0.0.1:8001",
            "payment_provider_timeout_seconds": 2.0,
            "payment_provider_max_attempts": 3,
            "payment_provider_retry_base_delay_seconds": 0.5,
            "payment_provider_failure_threshold": 3,
            "payment_provider_recovery_timeout_seconds": 30.0,
        }
    )

    order: list[str] = []

    async def check_database() -> None:
        order.append("database_checked")

    async def start_kafka() -> None:
        order.append("kafka_started")

    async def run_relay(stop_event: asyncio.Event) -> None:
        order.append("relay_started")

        await stop_event.wait()

        order.append("relay_failed")
        raise RuntimeError("Relay failed during shutdown")

    async def stop_kafka() -> None:
        order.append("kafka_stopped")

    async def close_http_client() -> None:
        order.append("http_client_closed")

    async def close_database() -> None:
        order.append("database_closed")

    raw_kafka_producer = MagicMock()

    kafka_producer = MagicMock()
    kafka_producer.start = AsyncMock(side_effect=start_kafka)
    kafka_producer.stop = AsyncMock(side_effect=stop_kafka)

    outbox_relay = MagicMock()
    outbox_relay.run = AsyncMock(side_effect=run_relay)

    http_client = MagicMock()
    http_client.aclose = AsyncMock(side_effect=close_http_client)

    session_factory = MagicMock()
    logger = MagicMock()

    close_db_mock = AsyncMock(side_effect=close_database)

    with (
        patch("app.main.Settings", return_value=settings),
        patch("app.main.configure_logging"),
        patch("app.main.init_db"),
        patch(
            "app.main.check_db_connection",
            new=AsyncMock(side_effect=check_database),
        ),
        patch(
            "app.main.close_db",
            new=close_db_mock,
        ),
        patch(
            "app.main.get_session_factory",
            return_value=session_factory,
        ),
        patch(
            "app.main.httpx.AsyncClient",
            return_value=http_client,
        ),
        patch(
            "app.main.AIOKafkaProducer",
            return_value=raw_kafka_producer,
        ),
        patch(
            "app.main.KafkaEventProducer",
            return_value=kafka_producer,
        ),
        patch(
            "app.main.OutboxRelay",
            return_value=outbox_relay,
        ),
        patch(
            "app.main.structlog.get_logger",
            return_value=logger,
        ),
        pytest.raises(
            RuntimeError,
            match="Relay failed during shutdown",
        ),
    ):
        async with lifespan(app):
            # Даём фоновой задаче relay начать выполнение.
            await asyncio.sleep(0)

            assert order == [
                "database_checked",
                "kafka_started",
                "relay_started",
            ]

    assert order == [
        "database_checked",
        "kafka_started",
        "relay_started",
        "relay_failed",
        "kafka_stopped",
        "http_client_closed",
        "database_closed",
    ]

    kafka_producer.stop.assert_awaited_once_with()
    http_client.aclose.assert_awaited_once_with()
    close_db_mock.assert_awaited_once_with()

    logger.info.assert_any_call(
        "service_stopped",
        service="payment-service",
        version="0.1.0",
    )
