from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from app.api.router import router
from app.core.config import Settings
from app.core.logging import configure_logging
from app.core.middleware import request_id_middleware


@asynccontextmanager
async def lifespan(_application: FastAPI) -> AsyncGenerator[None, None]:
    # Настройка логирования
    configure_logging()
    settings = Settings()  # pyright: ignore[reportCallIssue]
    logger = structlog.get_logger()

    # Логируем запуск приложения
    logger.info(
        "service_started",
        service="payment-service",
        version="0.1.0",
        host=settings.app_host,
        port=settings.app_port,
    )

    # Здесь можно добавить код для инициализации ресурсов, например, подключение к базе данных
    yield  # Передаем управление FastAPI для обработки запросов

    # Логируем завершение работы приложения
    logger.info("service_stopped", service="payment-service", version="0.1.0")

    # Здесь можно добавить код для очистки ресурсов, например, закрытие соединений с базой данных


# Создаем экземпляр FastAPI с указанием заголовка, версии и функции lifespan
app = FastAPI(
    title="PayFlow Payment Service",
    version="0.1.0",
    lifespan=lifespan,
)
# Добавляем middleware для обработки request_id и подключаем маршруты
app.middleware("http")(request_id_middleware)
# Подключаем маршруты из router.py
app.include_router(router)
