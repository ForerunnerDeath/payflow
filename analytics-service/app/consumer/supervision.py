import asyncio
from typing import Protocol

import structlog

from app.core.health import ConsumerHealthState

logger = structlog.get_logger()


class ConsumerRunner(Protocol):
    async def run(self, stop_event: asyncio.Event) -> None: ...


async def supervise_consumer(
    *,
    consumer: ConsumerRunner,
    stop_event: asyncio.Event,
    health_state: ConsumerHealthState,
) -> None:
    health_state.mark_running()

    try:
        await consumer.run(stop_event)
    except Exception as error:
        health_state.mark_failed(error)
        logger.exception("payment_event_consumer_task_failed")
        return

    if not stop_event.is_set():
        error = RuntimeError("Payment event consumer stopped unexpectedly")
        health_state.mark_failed(error)
        logger.error(
            "payment_event_consumer_task_stopped_unexpectedly",
            error=str(error),
        )
