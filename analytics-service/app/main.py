from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from app.api.router import router
from app.core.config import Settings
from app.core.logging import configure_logging
from app.core.middleware import request_id_middleware


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncGenerator[None, None]:
    # Настройка логирования
    configure_logging()

    settings = Settings()  # pyright: ignore[reportCallIssue]
    application.state.settings = settings

    logger = structlog.get_logger()

    logger.info(
        "service_started",
        service="analytics-service",
        version="0.1.0",
        host=settings.app_host,
        port=settings.app_port,
    )

    try:
        yield  # Передаем управление FastAPI для обработки запросов
    finally:
        logger.info(
            "service_stopped",
            service="analytics-service",
        )


# Создаем экземпляр FastAPI с указанием заголовка, версии и функции lifespan
app = FastAPI(
    title="PayFlow Transaction Analytics Service",
    version="0.1.0",
    lifespan=lifespan,
)
# Добавляем middleware для обработки request_id и подключаем маршруты
app.middleware("http")(request_id_middleware)
app.include_router(router)
