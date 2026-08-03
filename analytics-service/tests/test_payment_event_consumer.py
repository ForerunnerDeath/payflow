import asyncio
from dataclasses import dataclass
from typing import cast
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
from aiokafka.errors import IllegalStateError  # pyright: ignore[reportMissingTypeStubs]
from app.consumer.dead_letter_publisher import DeadLetterPublisher
from app.consumer.payment_event_consumer import PaymentEventConsumer
from app.services.payment_event_processor import PaymentEventProcessor


@dataclass(frozen=True)
class FakeKafkaMessage:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes


VALID_EVENT_JSON = b"""
{
    "event_id": "11111111-1111-1111-1111-111111111111",
    "event_type": "payment.completed",
    "payment_id": "22222222-2222-2222-2222-222222222222",
    "amount": "1500.50",
    "currency": "RUB",
    "status": "completed",
    "timestamp": "2026-08-03T10:00:00Z"
}
"""


@pytest.mark.asyncio
async def test_consumer_starts_and_stops_kafka_client() -> None:
    kafka_consumer = MagicMock()
    kafka_consumer.start = AsyncMock()
    kafka_consumer.stop = AsyncMock()
    processor = MagicMock(spec=PaymentEventProcessor)
    processor.process = AsyncMock()
    dead_letter_publisher = MagicMock(spec=DeadLetterPublisher)
    dead_letter_publisher.publish = AsyncMock()

    with patch(
        "app.consumer.payment_event_consumer.AIOKafkaConsumer",
        return_value=kafka_consumer,
    ) as consumer_class:
        consumer = PaymentEventConsumer(
            topic="payments",
            bootstrap_servers="localhost:29092",
            group_id="payflow-analytics-service",
            auto_offset_reset="earliest",
            processor=cast(PaymentEventProcessor, processor),
            dead_letter_publisher=cast(DeadLetterPublisher, dead_letter_publisher),
        )

        await consumer.start()
        await consumer.stop()

    consumer_class.assert_called_once_with(
        "payments",
        bootstrap_servers="localhost:29092",
        group_id="payflow-analytics-service",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )

    kafka_consumer.start.assert_awaited_once_with()
    kafka_consumer.stop.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_process_message_validates_and_forwards_event() -> None:
    processor = MagicMock(spec=PaymentEventProcessor)
    processor.process = AsyncMock()

    kafka_consumer = MagicMock()

    dead_letter_publisher = MagicMock(spec=DeadLetterPublisher)
    dead_letter_publisher.publish = AsyncMock()

    with patch(
        "app.consumer.payment_event_consumer.AIOKafkaConsumer",
        return_value=kafka_consumer,
    ):
        consumer = PaymentEventConsumer(
            topic="payments",
            bootstrap_servers="localhost:29092",
            group_id="payflow-analytics-service",
            auto_offset_reset="earliest",
            processor=cast(PaymentEventProcessor, processor),
            dead_letter_publisher=cast(DeadLetterPublisher, dead_letter_publisher),
        )

    value = VALID_EVENT_JSON

    event = await consumer.process_message(value)

    assert event.event_type == "payment.completed"
    assert event.status == "completed"
    processor.process.assert_awaited_once_with(event)


@pytest.mark.asyncio
async def test_handle_message_commits_offset_after_processing() -> None:
    processor = MagicMock(spec=PaymentEventProcessor)
    processor.process = AsyncMock()

    kafka_consumer = MagicMock()
    kafka_consumer.commit = AsyncMock()

    dead_letter_publisher = MagicMock(spec=DeadLetterPublisher)
    dead_letter_publisher.publish = AsyncMock()

    with patch(
        "app.consumer.payment_event_consumer.AIOKafkaConsumer",
        return_value=kafka_consumer,
    ):
        consumer = PaymentEventConsumer(
            topic="payments",
            bootstrap_servers="localhost:29092",
            group_id="payflow-analytics-service",
            auto_offset_reset="earliest",
            processor=cast(PaymentEventProcessor, processor),
            dead_letter_publisher=cast(DeadLetterPublisher, dead_letter_publisher),
        )

    message = FakeKafkaMessage(
        topic="payments",
        partition=1,
        offset=42,
        value=VALID_EVENT_JSON,
        key=b"payment-123",
    )

    event = await consumer.handle_message(message)

    processor.process.assert_awaited_once_with(event)
    kafka_consumer.commit.assert_awaited_once()

    committed_offsets = kafka_consumer.commit.await_args.args[0]
    topic_partition, committed_offset = next(iter(committed_offsets.items()))

    assert topic_partition.topic == "payments"
    assert topic_partition.partition == 1
    assert committed_offset == 43


@pytest.mark.asyncio
async def test_handle_message_does_not_commit_offset_when_processing_fails() -> None:
    processor = MagicMock(spec=PaymentEventProcessor)
    processor.process = AsyncMock(side_effect=RuntimeError("database failure"))

    kafka_consumer = MagicMock()
    kafka_consumer.commit = AsyncMock()

    dead_letter_publisher = MagicMock(spec=DeadLetterPublisher)
    dead_letter_publisher.publish = AsyncMock()

    with patch(
        "app.consumer.payment_event_consumer.AIOKafkaConsumer",
        return_value=kafka_consumer,
    ):
        consumer = PaymentEventConsumer(
            topic="payments",
            bootstrap_servers="localhost:29092",
            group_id="payflow-analytics-service",
            auto_offset_reset="earliest",
            processor=cast(PaymentEventProcessor, processor),
            dead_letter_publisher=cast(DeadLetterPublisher, dead_letter_publisher),
        )

    message = FakeKafkaMessage(
        topic="payments",
        partition=1,
        offset=42,
        value=VALID_EVENT_JSON,
        key=b"payment-123",
    )

    with pytest.raises(RuntimeError, match="database failure"):
        await consumer.handle_message(message)

    kafka_consumer.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_handle_message_sends_invalid_event_to_dead_letter_and_commits() -> None:
    processor = MagicMock(spec=PaymentEventProcessor)
    processor.process = AsyncMock()

    kafka_consumer = MagicMock()
    kafka_consumer.commit = AsyncMock()

    dead_letter_publisher = MagicMock(spec=DeadLetterPublisher)
    dead_letter_publisher.publish = AsyncMock()

    with patch(
        "app.consumer.payment_event_consumer.AIOKafkaConsumer",
        return_value=kafka_consumer,
    ):
        consumer = PaymentEventConsumer(
            topic="payments",
            bootstrap_servers="localhost:29092",
            group_id="payflow-analytics-service",
            auto_offset_reset="earliest",
            processor=cast(PaymentEventProcessor, processor),
            dead_letter_publisher=cast(DeadLetterPublisher, dead_letter_publisher),
        )

    invalid_value = b'{"event_id":'

    message = FakeKafkaMessage(
        topic="payments",
        partition=1,
        offset=42,
        key=b"payment-123",
        value=invalid_value,
    )

    event = await consumer.handle_message(message)

    assert event is None
    processor.process.assert_not_awaited()

    dead_letter_publisher.publish.assert_awaited_once()

    publish_call = dead_letter_publisher.publish.await_args
    assert publish_call is not None

    assert publish_call.kwargs["source_topic"] == "payments"
    assert publish_call.kwargs["source_partition"] == 1
    assert publish_call.kwargs["source_offset"] == 42
    assert publish_call.kwargs["source_key"] == b"payment-123"
    assert publish_call.kwargs["source_value"] == invalid_value

    kafka_consumer.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_handle_message_does_not_commit_when_dead_letter_publish_fails() -> None:
    processor = MagicMock(spec=PaymentEventProcessor)
    processor.process = AsyncMock()

    kafka_consumer = MagicMock()
    kafka_consumer.commit = AsyncMock()

    dead_letter_publisher = MagicMock(spec=DeadLetterPublisher)
    dead_letter_publisher.publish = AsyncMock(
        side_effect=RuntimeError("dead letter unavailable")
    )

    with patch(
        "app.consumer.payment_event_consumer.AIOKafkaConsumer",
        return_value=kafka_consumer,
    ):
        consumer = PaymentEventConsumer(
            topic="payments",
            bootstrap_servers="localhost:29092",
            group_id="payflow-analytics-service",
            auto_offset_reset="earliest",
            processor=cast(PaymentEventProcessor, processor),
            dead_letter_publisher=cast(DeadLetterPublisher, dead_letter_publisher),
        )

    message = FakeKafkaMessage(
        topic="payments",
        partition=1,
        offset=42,
        key=b"payment-123",
        value=b'{"event_id":',
    )

    with pytest.raises(RuntimeError, match="dead letter unavailable"):
        await consumer.handle_message(message)

    processor.process.assert_not_awaited()
    dead_letter_publisher.publish.assert_awaited_once()
    kafka_consumer.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_reads_batch_and_handles_message() -> None:
    processor = MagicMock(spec=PaymentEventProcessor)
    processor.process = AsyncMock()

    dead_letter_publisher = MagicMock(spec=DeadLetterPublisher)
    dead_letter_publisher.publish = AsyncMock()

    message = FakeKafkaMessage(
        topic="payments",
        partition=1,
        offset=42,
        key=b"payment-123",
        value=VALID_EVENT_JSON,
    )

    topic_partition = MagicMock()

    kafka_consumer = MagicMock()
    kafka_consumer.getmany = AsyncMock(
        return_value={
            topic_partition: [message],
        }
    )

    with patch(
        "app.consumer.payment_event_consumer.AIOKafkaConsumer",
        return_value=kafka_consumer,
    ):
        consumer = PaymentEventConsumer(
            topic="payments",
            bootstrap_servers="localhost:29092",
            group_id="payflow-analytics-service",
            auto_offset_reset="earliest",
            processor=cast(PaymentEventProcessor, processor),
            dead_letter_publisher=cast(
                DeadLetterPublisher,
                dead_letter_publisher,
            ),
        )

    stop_event = asyncio.Event()

    async def handle_and_stop(_: object) -> None:
        stop_event.set()

    handle_message = AsyncMock(side_effect=handle_and_stop)

    with patch.object(
        consumer,
        "handle_message",
        new=handle_message,
    ):
        await consumer.run(stop_event)

    kafka_consumer.getmany.assert_awaited_once_with(
        timeout_ms=1000,
    )
    handle_message.assert_awaited_once_with(message)


@pytest.mark.asyncio
async def test_run_seeks_failed_message_and_continues_other_partition() -> None:
    processor = MagicMock(spec=PaymentEventProcessor)
    processor.process = AsyncMock()

    dead_letter_publisher = MagicMock(spec=DeadLetterPublisher)
    dead_letter_publisher.publish = AsyncMock()

    failed_message = FakeKafkaMessage(
        topic="payments",
        partition=0,
        offset=10,
        key=b"payment-10",
        value=VALID_EVENT_JSON,
    )
    skipped_message = FakeKafkaMessage(
        topic="payments",
        partition=0,
        offset=11,
        key=b"payment-11",
        value=VALID_EVENT_JSON,
    )
    other_partition_message = FakeKafkaMessage(
        topic="payments",
        partition=1,
        offset=20,
        key=b"payment-20",
        value=VALID_EVENT_JSON,
    )

    partition_0 = MagicMock()
    partition_1 = MagicMock()

    kafka_consumer = MagicMock()
    kafka_consumer.getmany = AsyncMock(
        return_value={
            partition_0: [
                failed_message,
                skipped_message,
            ],
            partition_1: [
                other_partition_message,
            ],
        }
    )
    kafka_consumer.seek = MagicMock()

    with patch(
        "app.consumer.payment_event_consumer.AIOKafkaConsumer",
        return_value=kafka_consumer,
    ):
        consumer = PaymentEventConsumer(
            topic="payments",
            bootstrap_servers="localhost:29092",
            group_id="payflow-analytics-service",
            auto_offset_reset="earliest",
            processor=cast(PaymentEventProcessor, processor),
            dead_letter_publisher=cast(
                DeadLetterPublisher,
                dead_letter_publisher,
            ),
        )

    stop_event = asyncio.Event()

    async def handle_message(message: FakeKafkaMessage) -> None:
        if message is failed_message:
            raise RuntimeError("database failure")

        if message is other_partition_message:
            stop_event.set()

    handle_message_mock = AsyncMock(side_effect=handle_message)

    with patch.object(
        consumer,
        "handle_message",
        new=handle_message_mock,
    ):
        await consumer.run(stop_event)

    assert handle_message_mock.await_args_list == [
        call(failed_message),
        call(other_partition_message),
    ]

    kafka_consumer.seek.assert_called_once_with(
        partition_0,
        failed_message.offset,
    )


@pytest.mark.asyncio
async def test_run_stops_while_waiting_for_kafka_messages() -> None:
    processor = MagicMock(spec=PaymentEventProcessor)
    processor.process = AsyncMock()

    dead_letter_publisher = MagicMock(spec=DeadLetterPublisher)
    dead_letter_publisher.publish = AsyncMock()

    getmany_started = asyncio.Event()
    wait_forever = asyncio.Event()

    async def getmany_forever(
        *,
        timeout_ms: int,
    ) -> dict[object, list[FakeKafkaMessage]]:
        assert timeout_ms == 1000
        getmany_started.set()

        await wait_forever.wait()

        return {}

    kafka_consumer = MagicMock()
    kafka_consumer.getmany = AsyncMock(
        side_effect=getmany_forever,
    )

    with patch(
        "app.consumer.payment_event_consumer.AIOKafkaConsumer",
        return_value=kafka_consumer,
    ):
        consumer = PaymentEventConsumer(
            topic="payments",
            bootstrap_servers="localhost:29092",
            group_id="payflow-analytics-service",
            auto_offset_reset="earliest",
            processor=cast(PaymentEventProcessor, processor),
            dead_letter_publisher=cast(
                DeadLetterPublisher,
                dead_letter_publisher,
            ),
        )

    stop_event = asyncio.Event()

    run_task = asyncio.create_task(
        consumer.run(stop_event),
    )

    await getmany_started.wait()

    stop_event.set()

    await asyncio.wait_for(
        run_task,
        timeout=1.0,
    )

    kafka_consumer.getmany.assert_awaited_once_with(
        timeout_ms=1000,
    )


@pytest.mark.asyncio
async def test_run_continues_when_failed_partition_was_revoked() -> None:
    processor = MagicMock(spec=PaymentEventProcessor)
    processor.process = AsyncMock()

    dead_letter_publisher = MagicMock(spec=DeadLetterPublisher)
    dead_letter_publisher.publish = AsyncMock()

    revoked_partition_message = FakeKafkaMessage(
        topic="payments",
        partition=0,
        offset=10,
        key=b"payment-10",
        value=VALID_EVENT_JSON,
    )
    assigned_partition_message = FakeKafkaMessage(
        topic="payments",
        partition=1,
        offset=20,
        key=b"payment-20",
        value=VALID_EVENT_JSON,
    )

    revoked_partition = MagicMock()
    revoked_partition.topic = "payments"
    revoked_partition.partition = 0

    assigned_partition = MagicMock()
    assigned_partition.topic = "payments"
    assigned_partition.partition = 1

    kafka_consumer = MagicMock()
    kafka_consumer.getmany = AsyncMock(
        return_value={
            revoked_partition: [revoked_partition_message],
            assigned_partition: [assigned_partition_message],
        }
    )
    kafka_consumer.seek = MagicMock(
        side_effect=IllegalStateError(),
    )

    with patch(
        "app.consumer.payment_event_consumer.AIOKafkaConsumer",
        return_value=kafka_consumer,
    ):
        consumer = PaymentEventConsumer(
            topic="payments",
            bootstrap_servers="localhost:29092",
            group_id="payflow-analytics-service",
            auto_offset_reset="earliest",
            processor=cast(
                PaymentEventProcessor,
                processor,
            ),
            dead_letter_publisher=cast(
                DeadLetterPublisher,
                dead_letter_publisher,
            ),
        )

    stop_event = asyncio.Event()

    async def handle_message(
        message: FakeKafkaMessage,
    ) -> None:
        if message is revoked_partition_message:
            raise RuntimeError("commit failed during rebalance")

        if message is assigned_partition_message:
            stop_event.set()

    handle_message_mock = AsyncMock(
        side_effect=handle_message,
    )

    with patch.object(
        consumer,
        "handle_message",
        new=handle_message_mock,
    ):
        await consumer.run(stop_event)

    assert handle_message_mock.await_args_list == [
        call(revoked_partition_message),
        call(assigned_partition_message),
    ]

    kafka_consumer.seek.assert_called_once_with(
        revoked_partition,
        revoked_partition_message.offset,
    )
