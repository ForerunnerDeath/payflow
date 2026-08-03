from base64 import b64encode
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from app.consumer.dead_letter_publisher import DeadLetterPublisher
from app.schemas.dead_letter import DeadLetterMessage


@pytest.mark.asyncio
async def test_dead_letter_publisher_starts_and_stops_producer() -> None:
    kafka_producer = MagicMock()
    kafka_producer.start = AsyncMock()
    kafka_producer.stop = AsyncMock()

    with patch(
        "app.consumer.dead_letter_publisher.AIOKafkaProducer",
        return_value=kafka_producer,
    ) as producer_class:
        publisher = DeadLetterPublisher(
            topic="payments.dlq",
            bootstrap_servers="localhost:29092",
        )

        await publisher.start()
        await publisher.stop()

    producer_class.assert_called_once_with(
        bootstrap_servers="localhost:29092",
    )
    kafka_producer.start.assert_awaited_once_with()
    kafka_producer.stop.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_publish_sends_original_message_and_error_to_dead_letter_topic() -> None:
    kafka_producer = MagicMock()
    kafka_producer.send_and_wait = AsyncMock()

    with patch(
        "app.consumer.dead_letter_publisher.AIOKafkaProducer",
        return_value=kafka_producer,
    ):
        publisher = DeadLetterPublisher(
            topic="payments.dlq",
            bootstrap_servers="localhost:29092",
        )

    source_key = b"payment-123"
    source_value = b'{"broken": true'

    await publisher.publish(
        source_topic="payments",
        source_partition=1,
        source_offset=42,
        source_key=source_key,
        source_value=source_value,
        error=ValueError("invalid payment event"),
    )

    kafka_producer.send_and_wait.assert_awaited_once()

    call = kafka_producer.send_and_wait.await_args
    assert call is not None

    assert call.args[0] == "payments.dlq"
    assert call.kwargs["key"] == b"payments:1:42"

    message = DeadLetterMessage.model_validate_json(
        call.kwargs["value"],
    )

    assert message.source_topic == "payments"
    assert message.source_partition == 1
    assert message.source_offset == 42
    assert message.source_key_base64 == b64encode(source_key).decode("ascii")
    assert message.source_value_base64 == b64encode(source_value).decode("ascii")
    assert message.error_type == "ValueError"
    assert message.error_message == "invalid payment event"
