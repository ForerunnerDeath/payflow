from collections.abc import Awaitable, Callable
from enum import StrEnum
from time import monotonic
from typing import TypeVar

import httpx

ArgumentT = TypeVar("ArgumentT")
ResultT = TypeVar("ResultT")


class CircuitBreakerState(StrEnum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreakerOpenError(RuntimeError):
    pass


class CircuitBreaker:
    def __init__(self, failure_threshold: int, recovery_timeout_seconds: float) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be at least 1")
        if recovery_timeout_seconds < 0:
            raise ValueError("recovery_timeout_seconds must not be negative")
        self.failure_threshold = failure_threshold
        self.recovery_timeout_seconds = recovery_timeout_seconds
        self.state = CircuitBreakerState.CLOSED
        self.failure_count: int = 0
        self.opened_at: float | None = None

    def _before_call(self) -> None:
        if self.state == CircuitBreakerState.CLOSED:
            return
        if self.state == CircuitBreakerState.HALF_OPEN:
            raise CircuitBreakerOpenError("Circuit Breaker is open")
        if self.state == CircuitBreakerState.OPEN:
            if self.opened_at is None:
                raise RuntimeError("Circuit Breaker is open without opened_at time")
            elapsed = monotonic() - self.opened_at
            if elapsed < self.recovery_timeout_seconds:
                raise CircuitBreakerOpenError("Circuit Breaker is open")
            self.state = CircuitBreakerState.HALF_OPEN
            return

    def _record_success(self) -> None:
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.opened_at = None

    def _record_failure(self) -> None:
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.state = CircuitBreakerState.OPEN
            self.opened_at = monotonic()
            return

        if self.state == CircuitBreakerState.CLOSED:
            self.failure_count += 1
            if self.failure_count >= self.failure_threshold:
                self.state = CircuitBreakerState.OPEN
                self.opened_at = monotonic()

    async def call(
        self, operation: Callable[[ArgumentT], Awaitable[ResultT]], argument: ArgumentT
    ) -> ResultT:
        self._before_call()
        try:
            result = await operation(argument)
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            if 500 <= status_code < 600:
                self._record_failure()
            else:
                self._record_success()
            raise
        except httpx.RequestError:
            self._record_failure()
            raise
        except Exception:
            if self.state == CircuitBreakerState.HALF_OPEN:
                self._record_success()
            raise
        self._record_success()
        return result
