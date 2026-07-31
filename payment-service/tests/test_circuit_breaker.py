import httpx
import pytest
from app.clients.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerOpenError,
    CircuitBreakerState,
)


@pytest.mark.asyncio
async def test_successful_call_returns_result_and_keeps_breaker_closed() -> None:
    breaker = CircuitBreaker(
        failure_threshold=3,
        recovery_timeout_seconds=30,
    )

    async def successful_operation(value: str) -> str:
        return f"processed:{value}"

    result = await breaker.call(
        successful_operation,
        "payment-1",
    )

    assert result == "processed:payment-1"
    assert breaker.state == CircuitBreakerState.CLOSED
    assert breaker.failure_count == 0
    assert breaker.opened_at is None


@pytest.mark.asyncio
async def test_opens_after_failure_threshold_is_reached() -> None:
    breaker = CircuitBreaker(
        failure_threshold=3,
        recovery_timeout_seconds=30,
    )
    attempts = 0

    async def failing_operation(_: str) -> None:
        nonlocal attempts
        attempts += 1

        request = httpx.Request(
            method="POST",
            url="http://provider.test/process-payment",
        )
        response = httpx.Response(
            status_code=500,
            request=request,
        )
        response.raise_for_status()

    for expected_failure_count in (1, 2):
        with pytest.raises(httpx.HTTPStatusError):
            await breaker.call(
                failing_operation,
                "payment-1",
            )

        assert breaker.state == CircuitBreakerState.CLOSED
        assert breaker.failure_count == expected_failure_count

    with pytest.raises(httpx.HTTPStatusError):
        await breaker.call(
            failing_operation,
            "payment-1",
        )

    assert attempts == 3
    assert breaker.failure_count == 3
    assert breaker.state == CircuitBreakerState.OPEN
    assert breaker.opened_at is not None


@pytest.mark.asyncio
async def test_open_breaker_rejects_call_without_running_operation() -> None:
    breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout_seconds=30,
    )
    attempts = 0

    async def failing_operation(_: str) -> None:
        nonlocal attempts
        attempts += 1

        request = httpx.Request(
            method="POST",
            url="http://provider.test/process-payment",
        )
        response = httpx.Response(
            status_code=500,
            request=request,
        )
        response.raise_for_status()

    with pytest.raises(httpx.HTTPStatusError):
        await breaker.call(
            failing_operation,
            "payment-1",
        )

    assert breaker.state == CircuitBreakerState.OPEN
    assert attempts == 1

    with pytest.raises(CircuitBreakerOpenError):
        await breaker.call(
            failing_operation,
            "payment-2",
        )

    assert attempts == 1


@pytest.mark.asyncio
async def test_successful_half_open_call_closes_breaker() -> None:
    breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout_seconds=0,
    )

    async def failing_operation(_: str) -> None:
        request = httpx.Request(
            method="POST",
            url="http://provider.test/process-payment",
        )
        response = httpx.Response(
            status_code=500,
            request=request,
        )
        response.raise_for_status()

    async def successful_operation(value: str) -> str:
        assert breaker.state == CircuitBreakerState.HALF_OPEN
        return f"processed:{value}"

    with pytest.raises(httpx.HTTPStatusError):
        await breaker.call(
            failing_operation,
            "payment-1",
        )

    assert breaker.state == CircuitBreakerState.OPEN
    assert breaker.failure_count == 1
    assert breaker.opened_at is not None

    result = await breaker.call(
        successful_operation,
        "payment-2",
    )

    assert result == "processed:payment-2"
    assert breaker.state == CircuitBreakerState.CLOSED
    assert breaker.failure_count == 0
    assert breaker.opened_at is None
