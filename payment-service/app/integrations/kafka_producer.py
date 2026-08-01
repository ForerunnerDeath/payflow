import json
from typing import Protocol

from app.schemas.event import PaymentEventPayload


class KafkaProducerProtocol(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def send_and_wait(
        self, topic: str, value: bytes | None = None, key: bytes | None = None
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
