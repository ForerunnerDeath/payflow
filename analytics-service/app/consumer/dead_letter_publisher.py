from base64 import b64encode

import structlog
from aiokafka import AIOKafkaProducer  # pyright: ignore[reportMissingTypeStubs]

from app.schemas.dead_letter import DeadLetterMessage

logger = structlog.get_logger()


class DeadLetterPublisher:
    def __init__(self, *, topic: str, bootstrap_servers: str) -> None:
        self._topic = topic
        self._producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)

    async def start(self) -> None:
        await self._producer.start()

        logger.info(
            "dead_letter_publisher_started",
            topic=self._topic,
        )

    async def stop(self) -> None:
        await self._producer.stop()

        logger.info(
            "dead_letter_publisher_stopped",
            topic=self._topic,
        )

    async def publish(
        self,
        *,
        source_topic: str,
        source_partition: int,
        source_offset: int,
        source_key: bytes | None,
        source_value: bytes,
        error: Exception,
    ) -> None:
        message = DeadLetterMessage(
            source_topic=source_topic,
            source_partition=source_partition,
            source_offset=source_offset,
            source_key_base64=(
                b64encode(source_key).decode("ascii")
                if source_key is not None
                else None
            ),
            source_value_base64=b64encode(source_value).decode("ascii"),
            error_type=type(error).__name__,
            error_message=str(error),
        )

        deduplication_key = (
            f"{source_topic}:{source_partition}:{source_offset}".encode()
        )

        await self._producer.send_and_wait(  # pyright: ignore[reportUnknownMemberType]
            self._topic,
            value=message.model_dump_json().encode(),
            key=deduplication_key,
        )

        logger.warning(
            "payment_event_sent_to_dead_letter",
            source_topic=source_topic,
            source_partition=source_partition,
            source_offset=source_offset,
            error_type=type(error).__name__,
        )
