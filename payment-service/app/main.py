from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import httpx
import structlog
from fastapi import FastAPI

from app.api.router import router
from app.clients.circuit_breaker import CircuitBreaker
from app.clients.payment_provider import PaymentProviderClient
from app.core.config import Settings
from app.core.database import check_db_connection, close_db, init_db
from app.core.logging import configure_logging
from app.core.middleware import request_id_middleware


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncGenerator[None, None]:
    # Настройка логирования
    configure_logging()
    settings = Settings()  # pyright: ignore[reportCallIssue]
    init_db(settings)
    logger = structlog.get_logger()
    http_client = httpx.AsyncClient(
        base_url=settings.payment_provider_url,
        timeout=settings.payment_provider_timeout_seconds,
    )
    provider_client = PaymentProviderClient(
        client=http_client,
        max_attempts=settings.payment_provider_max_attempts,
        retry_base_delay_seconds=settings.payment_provider_retry_base_delay_seconds,
    )
    provider_circuit_breaker = CircuitBreaker(
        failure_threshold=settings.payment_provider_failure_threshold,
        recovery_timeout_seconds=settings.payment_provider_recovery_timeout_seconds,
    )
    application.state.payment_provider_client = provider_client
    application.state.payment_provider_circuit_breaker = provider_circuit_breaker
    try:
        await check_db_connection()  # Проверяем соединение с базой данных
        # Логируем запуск приложения
        logger.info(
            "service_started",
            service="payment-service",
            version="0.1.0",
            host=settings.app_host,
            port=settings.app_port,
        )
        yield  # Передаем управление FastAPI для обработки запросов
    finally:
        # Логируем завершение работы приложения и закрываем БД
        await http_client.aclose()
        await close_db()
        logger.info("service_stopped", service="payment-service", version="0.1.0")


# Создаем экземпляр FastAPI с указанием заголовка, версии и функции lifespan
app = FastAPI(
    title="PayFlow Payment Service",
    version="0.1.0",
    lifespan=lifespan,
)
# Добавляем middleware для обработки request_id и подключаем маршруты
app.middleware("http")(request_id_middleware)
app.include_router(router)
