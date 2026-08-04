from typing import Literal

from pydantic import BaseModel

DependencyStatus = Literal["ok", "unavailable"]

KafkaConsumerComponentStatus = Literal[
    "ok",
    "starting",
    "stopping",
    "stopped",
    "failed",
]

ReadinessStatus = Literal[
    "ready",
    "degraded",
    "not_ready",
]


class LivenessResponse(BaseModel):
    status: Literal["ok"] = "ok"


class ReadinessComponents(BaseModel):
    postgres: DependencyStatus
    kafka_consumer: KafkaConsumerComponentStatus
    redis: DependencyStatus


class ReadinessResponse(BaseModel):
    status: ReadinessStatus
    components: ReadinessComponents
