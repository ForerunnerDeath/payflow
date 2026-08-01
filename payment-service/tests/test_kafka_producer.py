import json
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from app.integrations.kafka_producer import KafkaEventProducer
from app.models.payment import PaymentStatus
from app.schemas.event import PaymentEventPayload


@pytest.mark.asyncio
async def test_start_starts_internal_producer() -> None:
    producer = AsyncMock()
    event_producer = KafkaEventProducer(
        topic="payments",
        producer=producer,
    )

    await event_producer.start()

    producer.start.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_stop_stops_internal_producer() -> None:
    producer = AsyncMock()
    event_producer = KafkaEventProducer(
        topic="payments",
        producer=producer,
    )

    await event_producer.stop()

    producer.stop.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_publish_sends_json_event_with_payment_id_key() -> None:
    producer = AsyncMock()
    event_producer = KafkaEventProducer(
        topic="payments",
        producer=producer,
    )

    event = PaymentEventPayload(
        event_id=UUID("11111111-1111-1111-1111-111111111111"),
        event_type="payment.completed",
        payment_id=UUID("22222222-2222-2222-2222-222222222222"),
        amount=Decimal("1500.50"),
        currency="RUB",
        status=PaymentStatus.COMPLETED,
        timestamp=datetime(2026, 8, 1, 10, 30, tzinfo=UTC),
    )

    await event_producer.publish(event)

    producer.send_and_wait.assert_awaited_once()

    call = producer.send_and_wait.await_args

    assert call.kwargs["topic"] == "payments"
    assert call.kwargs["key"] == (b"22222222-2222-2222-2222-222222222222")

    message = json.loads(call.kwargs["value"].decode("utf-8"))

    assert message == {
        "event_id": "11111111-1111-1111-1111-111111111111",
        "event_type": "payment.completed",
        "payment_id": "22222222-2222-2222-2222-222222222222",
        "amount": "1500.50",
        "currency": "RUB",
        "status": "completed",
        "timestamp": "2026-08-01T10:30:00Z",
    }


@pytest.mark.asyncio
async def test_check_connection_succeeds_when_topic_exists() -> None:
    producer = MagicMock()
    metadata = MagicMock()

    metadata.topics.return_value = {"payments", "another-topic"}
    producer.client.fetch_all_metadata = AsyncMock(return_value=metadata)

    event_producer = KafkaEventProducer(topic="payments", producer=producer)

    await event_producer.check_connection()

    producer.client.fetch_all_metadata.assert_awaited_once_with()
    metadata.topics.assert_called_once_with()


@pytest.mark.asyncio
async def test_check_connection_fails_when_topic_is_missing() -> None:
    producer = MagicMock()
    metadata = MagicMock()

    metadata.topics.return_value = {"another-topic"}
    producer.client.fetch_all_metadata = AsyncMock(return_value=metadata)

    event_producer = KafkaEventProducer(topic="payments", producer=producer)

    with pytest.raises(
        RuntimeError,
        match="Kafka topic 'payments' is unavailable",
    ):
        await event_producer.check_connection()


@pytest.mark.asyncio
async def test_check_connection_propagates_kafka_error() -> None:
    producer = MagicMock()
    producer.client.fetch_all_metadata = AsyncMock(
        side_effect=ConnectionError("Kafka is unavailable")
    )

    event_producer = KafkaEventProducer(topic="payments", producer=producer)

    with pytest.raises(ConnectionError, match="Kafka is unavailable"):
        await event_producer.check_connection()
