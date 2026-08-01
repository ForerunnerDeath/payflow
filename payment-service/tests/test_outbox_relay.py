import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest
from app.integrations.kafka_producer import KafkaEventProducer
from app.models.outbox_event import OutboxEvent
from app.models.payment import PaymentStatus
from app.schemas.event import PaymentEventPayload
from app.services.outbox_relay import OutboxRelay
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


def build_outbox_event(*, event_id: str, payment_id: str) -> OutboxEvent:
    payload = PaymentEventPayload(
        event_id=UUID(event_id),
        event_type="payment.completed",
        payment_id=UUID(payment_id),
        amount=Decimal("1500.50"),
        currency="RUB",
        status=PaymentStatus.COMPLETED,
        timestamp=datetime(2026, 8, 1, 10, 30, tzinfo=UTC),
    )

    return OutboxEvent(
        id=payload.event_id,
        event_type=payload.event_type,
        payload=payload.model_dump(mode="json"),
        published=False,
    )


@pytest.mark.asyncio
async def test_process_batch_publishes_events_and_commits() -> None:
    event_1 = build_outbox_event(
        event_id="11111111-1111-1111-1111-111111111111",
        payment_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    )
    event_2 = build_outbox_event(
        event_id="22222222-2222-2222-2222-222222222222",
        payment_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
    )

    session = AsyncMock()

    session_context = AsyncMock()
    session_context.__aenter__.return_value = session

    session_factory = MagicMock(return_value=session_context)
    producer = AsyncMock()

    repository = MagicMock()
    repository.get_unpublished_batch = AsyncMock(return_value=[event_1, event_2])

    relay = OutboxRelay(
        session_factory=cast(async_sessionmaker[AsyncSession], session_factory),
        producer=cast(KafkaEventProducer, producer),
        batch_size=10,
        poll_interval_seconds=1.0,
    )

    with patch(
        "app.services.outbox_relay.OutboxRepository", return_value=repository
    ) as repository_class:
        processed_count = await relay.process_batch()

    assert processed_count == 2

    repository_class.assert_called_once_with(session)
    repository.get_unpublished_batch.assert_awaited_once_with(10)

    assert producer.publish.await_count == 2

    repository.mark_published.assert_any_call(event_1)
    repository.mark_published.assert_any_call(event_2)

    session.commit.assert_awaited_once_with()
    session.rollback.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_batch_rolls_back_when_publish_fails() -> None:
    event_1 = build_outbox_event(
        event_id="11111111-1111-1111-1111-111111111111",
        payment_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    )
    event_2 = build_outbox_event(
        event_id="22222222-2222-2222-2222-222222222222",
        payment_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
    )

    session = AsyncMock()

    session_context = AsyncMock()
    session_context.__aenter__.return_value = session

    session_factory = MagicMock(return_value=session_context)

    producer = AsyncMock()
    producer.publish.side_effect = [None, RuntimeError("Kafka is unavailable")]

    repository = MagicMock()
    repository.get_unpublished_batch = AsyncMock(return_value=[event_1, event_2])

    relay = OutboxRelay(
        session_factory=cast(async_sessionmaker[AsyncSession], session_factory),
        producer=cast(KafkaEventProducer, producer),
        batch_size=10,
        poll_interval_seconds=1.0,
    )

    with (
        patch("app.services.outbox_relay.OutboxRepository", return_value=repository),
        pytest.raises(RuntimeError, match="Kafka is unavailable"),
    ):
        await relay.process_batch()

    assert producer.publish.await_count == 2

    repository.mark_published.assert_called_once_with(event_1)

    session.commit.assert_not_awaited()
    session.rollback.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_run_stops_after_stop_event_is_set() -> None:
    stop_event = asyncio.Event()

    session_factory = MagicMock()
    producer = AsyncMock()

    relay = OutboxRelay(
        session_factory=cast(async_sessionmaker[AsyncSession], session_factory),
        producer=cast(KafkaEventProducer, producer),
        batch_size=10,
        poll_interval_seconds=0.01,
    )

    async def process_once() -> int:
        stop_event.set()
        return 0

    process_batch = AsyncMock(side_effect=process_once)

    with patch.object(relay, "process_batch", process_batch):
        await relay.run(stop_event)

    process_batch.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_run_continues_after_batch_failure() -> None:
    stop_event = asyncio.Event()

    session_factory = MagicMock()
    producer = AsyncMock()

    relay = OutboxRelay(
        session_factory=cast(async_sessionmaker[AsyncSession], session_factory),
        producer=cast(KafkaEventProducer, producer),
        batch_size=10,
        poll_interval_seconds=0.001,
    )

    call_count = 0

    async def process_with_failure_then_success() -> int:
        nonlocal call_count
        call_count += 1

        if call_count == 1:
            raise RuntimeError("Kafka is unavailable")

        stop_event.set()
        return 1

    process_batch = AsyncMock(side_effect=process_with_failure_then_success)
    logger = MagicMock()

    with (
        patch.object(relay, "process_batch", process_batch),
        patch("app.services.outbox_relay.structlog.get_logger", return_value=logger),
    ):
        await relay.run(stop_event)

    assert process_batch.await_count == 2

    logger.exception.assert_called_once_with("outbox_batch_failed")
    logger.info.assert_any_call("outbox_batch_published", count=1)
    logger.info.assert_any_call("outbox_relay_stopped")
