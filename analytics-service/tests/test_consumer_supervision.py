import asyncio

import pytest
from app.consumer.supervision import supervise_consumer
from app.core.health import ConsumerHealthState, ConsumerStatus


class FailingConsumer:
    async def run(self, stop_event: asyncio.Event) -> None:
        del stop_event
        raise RuntimeError("Kafka connection lost")


class ReturningConsumer:
    async def run(self, stop_event: asyncio.Event) -> None:
        del stop_event


class GracefullyStoppingConsumer:
    async def run(self, stop_event: asyncio.Event) -> None:
        stop_event.set()


@pytest.mark.asyncio
async def test_supervise_consumer_marks_state_as_failed_on_exception() -> None:
    consumer = FailingConsumer()
    stop_event = asyncio.Event()
    health_state = ConsumerHealthState()

    await supervise_consumer(
        consumer=consumer,
        stop_event=stop_event,
        health_state=health_state,
    )

    assert health_state.status is ConsumerStatus.FAILED
    assert health_state.error == "Kafka connection lost"


@pytest.mark.asyncio
async def test_supervise_consumer_marks_state_as_failed_on_unexpected_return() -> None:
    consumer = ReturningConsumer()
    stop_event = asyncio.Event()
    health_state = ConsumerHealthState()

    await supervise_consumer(
        consumer=consumer,
        stop_event=stop_event,
        health_state=health_state,
    )

    assert health_state.status is ConsumerStatus.FAILED
    assert health_state.error == "Payment event consumer stopped unexpectedly"


@pytest.mark.asyncio
async def test_supervise_consumer_does_not_fail_on_graceful_stop() -> None:
    consumer = GracefullyStoppingConsumer()
    stop_event = asyncio.Event()
    health_state = ConsumerHealthState()

    await supervise_consumer(
        consumer=consumer,
        stop_event=stop_event,
        health_state=health_state,
    )

    assert stop_event.is_set()
    assert health_state.status is ConsumerStatus.RUNNING
    assert health_state.error is None
