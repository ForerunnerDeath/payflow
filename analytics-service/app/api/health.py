import asyncio
from typing import Protocol, cast

import structlog
from fastapi import APIRouter, Request, Response, status

from app.core.database import check_db_connection
from app.core.health import ConsumerHealthState, ConsumerStatus
from app.schemas.health import (
    DependencyStatus,
    KafkaConsumerComponentStatus,
    LivenessResponse,
    ReadinessComponents,
    ReadinessResponse,
    ReadinessStatus,
)

logger = structlog.get_logger(__name__)

REDIS_HEALTH_CHECK_TIMEOUT_SECONDS = 1.0


class RedisHealthClient(Protocol):
    async def ping(self) -> bool: ...


router = APIRouter(prefix="/health", tags=["health"])


async def _check_postgres() -> DependencyStatus:
    try:
        await check_db_connection()
    except Exception:
        logger.warning(
            "postgres_health_check_failed",
            exc_info=True,
        )
        return "unavailable"

    return "ok"


async def _check_redis(redis_client: RedisHealthClient) -> DependencyStatus:
    try:
        is_available = await asyncio.wait_for(
            redis_client.ping(),
            timeout=REDIS_HEALTH_CHECK_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        logger.warning(
            "redis_health_check_timed_out",
            timeout_seconds=REDIS_HEALTH_CHECK_TIMEOUT_SECONDS,
        )
        return "unavailable"
    except Exception:
        logger.warning(
            "redis_health_check_failed",
            exc_info=True,
        )
        return "unavailable"

    if not is_available:
        return "unavailable"

    return "ok"


def _get_consumer_component_status(
    consumer_status: ConsumerStatus,
) -> KafkaConsumerComponentStatus:
    match consumer_status:
        case ConsumerStatus.STARTING:
            return "starting"
        case ConsumerStatus.RUNNING:
            return "ok"
        case ConsumerStatus.STOPPING:
            return "stopping"
        case ConsumerStatus.STOPPED:
            return "stopped"
        case ConsumerStatus.FAILED:
            return "failed"

    raise AssertionError(f"Unsupported consumer status: {consumer_status}")


@router.get("/live", response_model=LivenessResponse)
async def get_liveness() -> LivenessResponse:
    return LivenessResponse()


@router.get(
    "/ready",
    response_model=ReadinessResponse,
    responses={
        status.HTTP_503_SERVICE_UNAVAILABLE: {
            "model": ReadinessResponse,
        }
    },
)
async def get_readiness(request: Request, response: Response) -> ReadinessResponse:
    redis_client = cast(RedisHealthClient, request.app.state.redis_client)
    consumer_health_state = cast(
        ConsumerHealthState,
        request.app.state.consumer_health_state,
    )

    postgres_status = await _check_postgres()
    redis_status = await _check_redis(redis_client)
    kafka_consumer_status = _get_consumer_component_status(consumer_health_state.status)

    readiness_status: ReadinessStatus

    if (
        postgres_status != "ok"
        or consumer_health_state.status is not ConsumerStatus.RUNNING
    ):
        readiness_status = "not_ready"
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    elif redis_status != "ok":
        readiness_status = "degraded"
    else:
        readiness_status = "ready"

    return ReadinessResponse(
        status=readiness_status,
        components=ReadinessComponents(
            postgres=postgres_status,
            kafka_consumer=kafka_consumer_status,
            redis=redis_status,
        ),
    )
