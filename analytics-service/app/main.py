import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from app.api.router import router
from app.consumer.dead_letter_publisher import DeadLetterPublisher
from app.consumer.payment_event_consumer import PaymentEventConsumer
from app.consumer.supervision import supervise_consumer
from app.core.config import Settings
from app.core.database import (
    check_db_connection,
    close_db,
    get_session_factory,
    init_db,
)
from app.core.health import ConsumerHealthState
from app.core.logging import configure_logging
from app.core.middleware import request_id_middleware
from app.services.analytics_cache import AnalyticsSummaryCache, RedisClientAdapter
from app.services.payment_event_processor import PaymentEventProcessor


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncGenerator[None, None]:
    # Настройка логирования
    configure_logging()
    settings = Settings()  # pyright: ignore[reportCallIssue]
    application.state.settings = settings
    logger = structlog.get_logger()
    init_db(settings)

    redis_client = RedisClientAdapter.from_url(settings.redis_url)
    application.state.redis_client = redis_client
    summary_cache = AnalyticsSummaryCache(
        redis=redis_client,
        ttl_seconds=settings.analytics_summary_cache_ttl_seconds,
    )
    application.state.analytics_summary_cache = summary_cache

    processor = PaymentEventProcessor(
        session_factory=get_session_factory(), summary_cache=summary_cache
    )
    application.state.payment_event_processor = processor

    consumer_health_state = ConsumerHealthState()
    application.state.consumer_health_state = consumer_health_state

    dead_letter_publisher: DeadLetterPublisher | None = None
    consumer: PaymentEventConsumer | None = None
    consumer_stop_event = asyncio.Event()
    consumer_task: asyncio.Task[None] | None = None

    try:
        await check_db_connection()

        dead_letter_publisher = DeadLetterPublisher(
            topic=settings.kafka_dlq_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
        )
        application.state.dead_letter_publisher = dead_letter_publisher

        await dead_letter_publisher.start()
        consumer = PaymentEventConsumer(
            topic=settings.kafka_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_consumer_group,
            auto_offset_reset=settings.kafka_auto_offset_reset,
            processor=processor,
            dead_letter_publisher=dead_letter_publisher,
        )
        application.state.payment_event_consumer = consumer

        await consumer.start()

        consumer_task = asyncio.create_task(
            supervise_consumer(
                consumer=consumer,
                stop_event=consumer_stop_event,
                health_state=consumer_health_state,
            ),
            name="payment-event-consumer",
        )

        logger.info(
            "service_started",
            service="analytics-service",
            version="0.1.0",
            host=settings.app_host,
            port=settings.app_port,
        )

        yield  # Передаем управление FastAPI для обработки запросов

    finally:
        consumer_health_state.mark_stopping()
        consumer_stop_event.set()

        if consumer_task is not None:
            try:
                await asyncio.wait_for(asyncio.shield(consumer_task), timeout=5.0)
            except TimeoutError:
                logger.warning(
                    "payment_event_consumer_task_stop_timeout", timeout_seconds=5.0
                )

                consumer_task.cancel()

                try:
                    await consumer_task
                except asyncio.CancelledError:
                    logger.info("payment_event_consumer_task_cancelled")
                except Exception:
                    logger.exception("payment_event_consumer_task_failed_after_cancel")
            except Exception:
                logger.exception("payment_event_consumer_task_failed")

        if consumer is not None:
            try:
                await consumer.stop()
            except Exception:
                logger.exception("payment_event_consumer_stop_failed")

        consumer_health_state.mark_stopped()

        if dead_letter_publisher is not None:
            try:
                await dead_letter_publisher.stop()
            except Exception:
                logger.exception("dead_letter_publisher_stop_failed")

        try:
            await redis_client.aclose()
        except Exception:
            logger.exception("redis_close_failed")

        try:
            await close_db()
        except Exception:
            logger.exception("database_close_failed")

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
