import app.api.health as health_module
import pytest
from app.core.health import ConsumerHealthState
from app.main import app
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient


class HealthyRedisClient:
    async def ping(self) -> bool:
        return True


class UnavailableRedisClient:
    async def ping(self) -> bool:
        raise ConnectionError("Redis unavailable")


async def successful_db_check() -> None:
    return None


async def failed_db_check() -> None:
    raise ConnectionError("PostgreSQL unavailable")


def configure_health_state(
    application: FastAPI,
    *,
    redis_client: object,
    consumer_health_state: ConsumerHealthState,
) -> None:
    application.state.redis_client = redis_client
    application.state.consumer_health_state = consumer_health_state


@pytest.mark.asyncio
async def test_liveness_returns_ok() -> None:
    transport = ASGITransport(app=app)

    async with AsyncClient(
        transport=transport,
        base_url="http://test",
    ) as client:
        response = await client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
    }


@pytest.mark.asyncio
async def test_readiness_returns_ready_when_all_components_are_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        health_module,
        "check_db_connection",
        successful_db_check,
    )

    consumer_health_state = ConsumerHealthState()
    consumer_health_state.mark_running()

    configure_health_state(
        app,
        redis_client=HealthyRedisClient(),
        consumer_health_state=consumer_health_state,
    )

    transport = ASGITransport(app=app)

    async with AsyncClient(
        transport=transport,
        base_url="http://test",
    ) as client:
        response = await client.get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ready",
        "components": {
            "postgres": "ok",
            "kafka_consumer": "ok",
            "redis": "ok",
        },
    }


@pytest.mark.asyncio
async def test_readiness_returns_not_ready_when_postgres_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        health_module,
        "check_db_connection",
        failed_db_check,
    )

    consumer_health_state = ConsumerHealthState()
    consumer_health_state.mark_running()

    configure_health_state(
        app,
        redis_client=HealthyRedisClient(),
        consumer_health_state=consumer_health_state,
    )

    transport = ASGITransport(app=app)

    async with AsyncClient(
        transport=transport,
        base_url="http://test",
    ) as client:
        response = await client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {
        "status": "not_ready",
        "components": {
            "postgres": "unavailable",
            "kafka_consumer": "ok",
            "redis": "ok",
        },
    }


@pytest.mark.asyncio
async def test_readiness_returns_not_ready_when_consumer_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        health_module,
        "check_db_connection",
        successful_db_check,
    )

    consumer_health_state = ConsumerHealthState()
    consumer_health_state.mark_failed(RuntimeError("Kafka consumer task failed"))

    configure_health_state(
        app,
        redis_client=HealthyRedisClient(),
        consumer_health_state=consumer_health_state,
    )

    transport = ASGITransport(app=app)

    async with AsyncClient(
        transport=transport,
        base_url="http://test",
    ) as client:
        response = await client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {
        "status": "not_ready",
        "components": {
            "postgres": "ok",
            "kafka_consumer": "failed",
            "redis": "ok",
        },
    }


@pytest.mark.asyncio
async def test_readiness_returns_degraded_when_redis_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        health_module,
        "check_db_connection",
        successful_db_check,
    )

    consumer_health_state = ConsumerHealthState()
    consumer_health_state.mark_running()

    configure_health_state(
        app,
        redis_client=UnavailableRedisClient(),
        consumer_health_state=consumer_health_state,
    )

    transport = ASGITransport(app=app)

    async with AsyncClient(
        transport=transport,
        base_url="http://test",
    ) as client:
        response = await client.get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {
        "status": "degraded",
        "components": {
            "postgres": "ok",
            "kafka_consumer": "ok",
            "redis": "unavailable",
        },
    }


@pytest.mark.asyncio
async def test_readiness_returns_not_ready_when_consumer_is_starting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        health_module,
        "check_db_connection",
        successful_db_check,
    )

    consumer_health_state = ConsumerHealthState()

    configure_health_state(
        app,
        redis_client=HealthyRedisClient(),
        consumer_health_state=consumer_health_state,
    )

    transport = ASGITransport(app=app)

    async with AsyncClient(
        transport=transport,
        base_url="http://test",
    ) as client:
        response = await client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {
        "status": "not_ready",
        "components": {
            "postgres": "ok",
            "kafka_consumer": "starting",
            "redis": "ok",
        },
    }
