from dataclasses import dataclass
from enum import StrEnum


class ConsumerStatus(StrEnum):
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"


@dataclass(slots=True)
class ConsumerHealthState:
    status: ConsumerStatus = ConsumerStatus.STARTING
    error: str | None = None

    def mark_running(self) -> None:
        self.status = ConsumerStatus.RUNNING
        self.error = None

    def mark_stopping(self) -> None:
        self.status = ConsumerStatus.STOPPING

    def mark_stopped(self) -> None:
        self.status = ConsumerStatus.STOPPED

    def mark_failed(self, error: Exception) -> None:
        self.status = ConsumerStatus.FAILED
        self.error = str(error)
