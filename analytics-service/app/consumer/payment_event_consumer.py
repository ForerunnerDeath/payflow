import asyncio
from collections.abc import Mapping, Sequence
from typing import Protocol, cast

import structlog
from aiokafka import AIOKafkaConsumer  # pyright: ignore[reportMissingTypeStubs]
from aiokafka.errors import IllegalStateError  # pyright: ignore[reportMissingTypeStubs]
from aiokafka.structs import TopicPartition  # pyright: ignore[reportMissingTypeStubs]
from pydantic import ValidationError

from app.consumer.dead_letter_publisher import DeadLetterPublisher
from app.schemas.event import PaymentEvent
from app.services.payment_event_processor import PaymentEventProcessor

logger = structlog.get_logger()

POLL_TIMEOUT_MS = 1000
RETRY_DELAY_SECONDS = 1.0


class KafkaMessage(Protocol):
    @property
    def topic(self) -> str: ...

    @property
    def partition(self) -> int: ...

    @property
    def offset(self) -> int: ...

    @property
    def key(self) -> bytes | None: ...

    @property
    def value(self) -> bytes: ...


class KafkaTopicPartition(Protocol):
    @property
    def topic(self) -> str: ...

    @property
    def partition(self) -> int: ...


KafkaBatch = Mapping[KafkaTopicPartition, Sequence[KafkaMessage]]


class KafkaConsumerClient(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def getmany(self, *, timeout_ms: int) -> KafkaBatch: ...

    async def commit(self, offsets: Mapping[KafkaTopicPartition, int]) -> None: ...

    def seek(self, partition: KafkaTopicPartition, offset: int) -> None: ...


class PaymentEventConsumer:
    def __init__(
        self,
        *,
        topic: str,
        bootstrap_servers: str,
        group_id: str,
        auto_offset_reset: str,
        processor: PaymentEventProcessor,
        dead_letter_publisher: DeadLetterPublisher,
    ) -> None:
        self._topic = topic
        self._group_id = group_id
        self._processor = processor
        self._dead_letter_publisher = dead_letter_publisher

        self._consumer = cast(
            KafkaConsumerClient,
            AIOKafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                enable_auto_commit=False,
                auto_offset_reset=auto_offset_reset,
            ),
        )

    async def start(self) -> None:
        await self._consumer.start()

        logger.info(
            "payment_event_consumer_started",
            topic=self._topic,
            group_id=self._group_id,
        )

    async def stop(self) -> None:
        await self._consumer.stop()

        logger.info(
            "payment_event_consumer_stopped",
            topic=self._topic,
            group_id=self._group_id,
        )

    async def _get_batches_or_stop(
        self, stop_event: asyncio.Event
    ) -> KafkaBatch | None:
        getmany_task = asyncio.create_task(
            self._consumer.getmany(timeout_ms=POLL_TIMEOUT_MS)
        )
        stop_task = asyncio.create_task(stop_event.wait())

        try:
            done_tasks, _ = await asyncio.wait(
                {
                    getmany_task,
                    stop_task,
                },
                return_when=asyncio.FIRST_COMPLETED,
            )

            if stop_task in done_tasks:
                return None

            return await getmany_task
        finally:
            for task in (getmany_task, stop_task):
                if not task.done():
                    task.cancel()

            await asyncio.gather(
                getmany_task,
                stop_task,
                return_exceptions=True,
            )

    def _seek_to_failed_message(
        self, topic_partition: KafkaTopicPartition, offset: int
    ) -> bool:
        try:
            self._consumer.seek(topic_partition, offset)
        except IllegalStateError:
            logger.warning(
                "payment_event_retry_seek_skipped",
                topic=topic_partition.topic,
                partition=topic_partition.partition,
                offset=offset,
                reason="partition_not_assigned",
            )

            return False

        return True

    async def run(self, stop_event: asyncio.Event) -> None:
        logger.info(
            "payment_event_consumer_loop_started",
            topic=self._topic,
            group_id=self._group_id,
        )

        try:
            while not stop_event.is_set():
                batches = await self._get_batches_or_stop(stop_event)

                if batches is None:
                    return

                retry_required = False

                for topic_partition, messages in batches.items():
                    for message in messages:
                        if stop_event.is_set():
                            return

                        try:
                            await self.handle_message(message)
                        except Exception:
                            logger.exception(
                                "payment_event_processing_failed",
                                topic=message.topic,
                                partition=message.partition,
                                offset=message.offset,
                            )

                            partition_retry_scheduled = self._seek_to_failed_message(
                                topic_partition,
                                message.offset,
                            )

                            retry_required = retry_required or partition_retry_scheduled

                            break

                if retry_required and not stop_event.is_set():
                    try:
                        await asyncio.wait_for(
                            stop_event.wait(),
                            timeout=RETRY_DELAY_SECONDS,
                        )
                    except TimeoutError:
                        pass
        finally:
            logger.info(
                "payment_event_consumer_loop_stopped",
                topic=self._topic,
                group_id=self._group_id,
            )

    async def process_message(self, value: bytes) -> tuple[PaymentEvent, bool]:
        event = PaymentEvent.model_validate_json(value)

        processed = await self._processor.process(event)

        return event, processed

    async def handle_message(self, message: KafkaMessage) -> PaymentEvent | None:
        try:
            event, processed = await self.process_message(message.value)
        except ValidationError as error:
            await self._dead_letter_publisher.publish(
                source_topic=message.topic,
                source_partition=message.partition,
                source_offset=message.offset,
                source_key=message.key,
                source_value=message.value,
                error=error,
            )

            await self._commit_message(message)

            logger.warning(
                "invalid_payment_event_skipped",
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
            )

            return None

        await self._commit_message(message)

        if processed:
            logger.info(
                "payment_event_processed",
                event_id=str(event.event_id),
                payment_id=str(event.payment_id),
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
            )
        else:
            logger.info(
                "duplicate_payment_event_skipped",
                event_id=str(event.event_id),
                payment_id=str(event.payment_id),
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
            )

        return event

    async def _commit_message(self, message: KafkaMessage) -> None:
        topic_partition = cast(
            KafkaTopicPartition,
            TopicPartition(
                message.topic,
                message.partition,
            ),
        )

        await self._consumer.commit(
            {
                topic_partition: message.offset + 1,
            }
        )
