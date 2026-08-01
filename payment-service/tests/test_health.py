from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from app.integrations.kafka_producer import KafkaEventProducer
from app.main import app
from httpx import ASGITransport, AsyncClient
from sqlalchemy.exc import SQLAlchemyError


@pytest.mark.asyncio
async def test_liveness_returns_ok() -> None:
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_readiness_returns_ok_when_dependencies_are_available() -> None:
    db_check = AsyncMock(return_value=None)

    kafka_producer = MagicMock(spec=KafkaEventProducer)
    kafka_producer.check_connection = AsyncMock(return_value=None)
    app.state.kafka_producer = kafka_producer

    with patch("app.api.health.check_db_connection", new=db_check):
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {
        "postgres": "ok",
        "kafka": "ok",
    }

    db_check.assert_awaited_once_with()
    kafka_producer.check_connection.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_readiness_returns_503_when_postgres_is_unavailable() -> None:
    db_check = AsyncMock(side_effect=SQLAlchemyError("PostgreSQL is unavailable"))

    kafka_producer = MagicMock(spec=KafkaEventProducer)
    kafka_producer.check_connection = AsyncMock(return_value=None)
    app.state.kafka_producer = kafka_producer

    with patch("app.api.health.check_db_connection", new=db_check):
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {
        "postgres": "unavailable",
        "kafka": "ok",
    }

    kafka_producer.check_connection.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_readiness_returns_503_when_kafka_is_unavailable() -> None:
    db_check = AsyncMock(return_value=None)

    kafka_producer = MagicMock(spec=KafkaEventProducer)
    kafka_producer.check_connection = AsyncMock(
        side_effect=RuntimeError("Kafka is unavailable")
    )
    app.state.kafka_producer = kafka_producer

    with patch("app.api.health.check_db_connection", new=db_check):
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {
        "postgres": "ok",
        "kafka": "unavailable",
    }

    db_check.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_readiness_checks_both_dependencies_when_both_are_unavailable() -> None:
    db_check = AsyncMock(side_effect=SQLAlchemyError("PostgreSQL is unavailable"))

    kafka_producer = MagicMock(spec=KafkaEventProducer)
    kafka_producer.check_connection = AsyncMock(
        side_effect=RuntimeError("Kafka is unavailable")
    )
    app.state.kafka_producer = kafka_producer

    with patch("app.api.health.check_db_connection", new=db_check):
        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {
        "postgres": "unavailable",
        "kafka": "unavailable",
    }

    db_check.assert_awaited_once_with()
    kafka_producer.check_connection.assert_awaited_once_with()
