from typing import cast

from fastapi import APIRouter, Request, Response, status
from sqlalchemy.exc import SQLAlchemyError

from app.core.database import check_db_connection
from app.integrations.kafka_producer import KafkaEventProducer

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/live")
async def live() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready")
async def ready(request: Request, response: Response) -> dict[str, str]:
    checks = {"postgres": "ok", "kafka": "ok"}
    try:
        await check_db_connection()
    except (SQLAlchemyError, RuntimeError, OSError):
        checks["postgres"] = "unavailable"
    kafka_producer = cast(KafkaEventProducer, request.app.state.kafka_producer)
    try:
        await kafka_producer.check_connection()
    except RuntimeError:
        checks["kafka"] = "unavailable"
    if "unavailable" in checks.values():
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return checks
