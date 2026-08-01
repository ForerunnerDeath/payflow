import json
from typing import Protocol

from app.schemas.event import PaymentEventPayload


class KafkaMetadataProtocol(Protocol):
    def topics(self) -> set[str]: ...


class KafkaClientProtocol(Protocol):
    async def fetch_all_metadata(self) -> KafkaMetadataProtocol: ...


class KafkaProducerProtocol(Protocol):
    @property
    def client(self) -> KafkaClientProtocol: ...

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def send_and_wait(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
    ) -> object: ...


class KafkaEventProducer:
    def __init__(self, topic: str, producer: KafkaProducerProtocol) -> None:
        self._topic = topic
        self._producer = producer

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def publish(self, event: PaymentEventPayload) -> None:
        message = json.dumps(event.model_dump(mode="json")).encode("utf-8")
        key = str(event.payment_id).encode("utf-8")
        await self._producer.send_and_wait(topic=self._topic, value=message, key=key)

    async def check_connection(self) -> None:
        metadata = await self._producer.client.fetch_all_metadata()
        if self._topic not in metadata.topics():
            raise RuntimeError(f"Kafka topic {self._topic!r} is unavailable.")
