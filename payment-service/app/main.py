import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import cast

import httpx
import structlog
from aiokafka import AIOKafkaProducer  # pyright: ignore[reportMissingTypeStubs]
from fastapi import FastAPI

from app.api.router import router
from app.clients.circuit_breaker import CircuitBreaker
from app.clients.payment_provider import PaymentProviderClient
from app.core.config import Settings
from app.core.database import (
    check_db_connection,
    close_db,
    get_session_factory,
    init_db,
)
from app.core.logging import configure_logging
from app.core.middleware import request_id_middleware
from app.integrations.kafka_producer import KafkaEventProducer, KafkaProducerProtocol
from app.services.outbox_relay import OutboxRelay


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

    raw_kafka_producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers
    )
    kafka_producer = KafkaEventProducer(
        topic=settings.kafka_topic,
        producer=cast(KafkaProducerProtocol, raw_kafka_producer),
    )
    outbox_stop_event = asyncio.Event()
    outbox_relay = OutboxRelay(
        session_factory=get_session_factory(),
        producer=kafka_producer,
        batch_size=settings.outbox_relay_batch_size,
        poll_interval_seconds=settings.outbox_relay_poll_interval_seconds,
    )
    relay_task: asyncio.Task[None] | None = None
    kafka_producer_started = False

    try:
        await check_db_connection()  # Проверяем соединение с базой данных
        await kafka_producer.start()  # Запускаем Kafka producer
        kafka_producer_started = True
        application.state.kafka_producer = kafka_producer
        relay_task = asyncio.create_task(
            outbox_relay.run(outbox_stop_event), name="outbox_relay"
        )
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
        outbox_stop_event.set()  # Останавливаем OutboxRelay
        try:
            if relay_task is not None:
                await relay_task
        finally:
            try:
                if kafka_producer_started:
                    await kafka_producer.stop()  # Останавливаем Kafka producer
            finally:
                try:
                    await http_client.aclose()  # Закрываем HTTP клиент
                finally:
                    try:
                        await close_db()  # Закрываем соединение с базой данных
                    finally:
                        logger.info(
                            "service_stopped",
                            service="payment-service",
                            version="0.1.0",
                        )


# Создаем экземпляр FastAPI с указанием заголовка, версии и функции lifespan
app = FastAPI(
    title="PayFlow Payment Service",
    version="0.1.0",
    lifespan=lifespan,
)
# Добавляем middleware для обработки request_id и подключаем маршруты
app.middleware("http")(request_id_middleware)
app.include_router(router)
