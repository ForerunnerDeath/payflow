import asyncio
from unittest.mock import Mock, call

import app.clients.circuit_breaker as circuit_breaker_module
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
async def test_opens_after_failure_threshold_is_reached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger_mock = Mock()
    monkeypatch.setattr(circuit_breaker_module, "logger", logger_mock)
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

    logger_mock.warning.assert_called_once_with(
        "circuit_breaker_state_changed",
        previous_state=CircuitBreakerState.CLOSED.value,
        new_state=CircuitBreakerState.OPEN.value,
        reason="failure_threshold_reached",
        failure_count=3,
        failure_threshold=3,
    )


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
async def test_successful_half_open_call_closes_breaker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger_mock = Mock()
    monkeypatch.setattr(circuit_breaker_module, "logger", logger_mock)

    timer_values = iter([100.0, 100.25])
    monkeypatch.setattr(
        circuit_breaker_module,
        "monotonic",
        lambda: next(timer_values),
    )
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

    assert logger_mock.info.call_args_list == [
        call(
            "circuit_breaker_state_changed",
            previous_state=CircuitBreakerState.OPEN.value,
            new_state=CircuitBreakerState.HALF_OPEN.value,
            reason="recovery_timeout_elapsed",
            elapsed_seconds=0.25,
            recovery_timeout_seconds=0,
        ),
        call(
            "circuit_breaker_state_changed",
            previous_state=CircuitBreakerState.HALF_OPEN.value,
            new_state=CircuitBreakerState.CLOSED.value,
            reason="trial_call_succeeded",
        ),
    ]


@pytest.mark.asyncio
async def test_failed_half_open_call_reopens_breaker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger_mock = Mock()
    monkeypatch.setattr(circuit_breaker_module, "logger", logger_mock)

    timer_values = iter([100.0, 100.25, 100.5])
    monkeypatch.setattr(
        circuit_breaker_module,
        "monotonic",
        lambda: next(timer_values),
    )

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

    # Первая ошибка открывает Circuit Breaker
    with pytest.raises(httpx.HTTPStatusError):
        await breaker.call(failing_operation, "payment-1")

    assert breaker.state == CircuitBreakerState.OPEN

    # Следующий вызов становится пробным и тоже завершается ошибкой
    with pytest.raises(httpx.HTTPStatusError):
        await breaker.call(failing_operation, "payment-2")

    assert breaker.state == CircuitBreakerState.OPEN
    assert breaker.opened_at == 100.5

    logger_mock.info.assert_called_once_with(
        "circuit_breaker_state_changed",
        previous_state=CircuitBreakerState.OPEN.value,
        new_state=CircuitBreakerState.HALF_OPEN.value,
        reason="recovery_timeout_elapsed",
        elapsed_seconds=0.25,
        recovery_timeout_seconds=0,
    )

    assert logger_mock.warning.call_args_list == [
        call(
            "circuit_breaker_state_changed",
            previous_state=CircuitBreakerState.CLOSED.value,
            new_state=CircuitBreakerState.OPEN.value,
            reason="failure_threshold_reached",
            failure_count=1,
            failure_threshold=1,
        ),
        call(
            "circuit_breaker_state_changed",
            previous_state=CircuitBreakerState.HALF_OPEN.value,
            new_state=CircuitBreakerState.OPEN.value,
            reason="trial_call_failed",
            recovery_timeout_seconds=0,
        ),
    ]


@pytest.mark.asyncio
async def test_stale_regular_success_does_not_close_half_open_breaker() -> None:
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout_seconds=0)

    old_call_started = asyncio.Event()
    release_old_call = asyncio.Event()

    probe_started = asyncio.Event()
    release_probe = asyncio.Event()

    async def slow_successful_operation(value: str) -> str:
        old_call_started.set()
        await release_old_call.wait()
        return f"processed:{value}"

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

    async def successful_probe(value: str) -> str:
        probe_started.set()
        await release_probe.wait()
        return f"processed:{value}"

    old_call_task = asyncio.create_task(
        breaker.call(slow_successful_operation, "old-payment")
    )

    await old_call_started.wait()

    with pytest.raises(httpx.HTTPStatusError):
        await breaker.call(failing_operation, "failed-payment")

    assert breaker.state == CircuitBreakerState.OPEN

    probe_task = asyncio.create_task(breaker.call(successful_probe, "probe-payment"))

    await probe_started.wait()

    assert breaker.state == CircuitBreakerState.HALF_OPEN

    release_old_call.set()
    old_result = await old_call_task

    assert old_result == "processed:old-payment"
    assert breaker.state == CircuitBreakerState.HALF_OPEN

    release_probe.set()
    probe_result = await probe_task

    assert probe_result == "processed:probe-payment"
    assert breaker.state == CircuitBreakerState.CLOSED


@pytest.mark.asyncio
async def test_stale_regular_failure_does_not_reopen_half_open_breaker() -> None:
    breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout_seconds=0,
    )

    old_call_started = asyncio.Event()
    release_old_call = asyncio.Event()

    probe_started = asyncio.Event()
    release_probe = asyncio.Event()

    async def slow_failing_operation(_: str) -> None:
        old_call_started.set()
        await release_old_call.wait()

        request = httpx.Request(
            method="POST",
            url="http://provider.test/process-payment",
        )
        response = httpx.Response(
            status_code=500,
            request=request,
        )
        response.raise_for_status()

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

    async def successful_probe(value: str) -> str:
        probe_started.set()
        await release_probe.wait()
        return f"processed:{value}"

    old_call_task = asyncio.create_task(
        breaker.call(slow_failing_operation, "old-payment")
    )

    await old_call_started.wait()

    with pytest.raises(httpx.HTTPStatusError):
        await breaker.call(failing_operation, "failed-payment")

    assert breaker.state == CircuitBreakerState.OPEN

    probe_task = asyncio.create_task(breaker.call(successful_probe, "probe-payment"))

    await probe_started.wait()

    assert breaker.state == CircuitBreakerState.HALF_OPEN

    release_old_call.set()

    with pytest.raises(httpx.HTTPStatusError):
        await old_call_task

    assert breaker.state == CircuitBreakerState.HALF_OPEN

    release_probe.set()
    probe_result = await probe_task

    assert probe_result == "processed:probe-payment"
    assert breaker.state == CircuitBreakerState.CLOSED


@pytest.mark.asyncio
async def test_cancelled_half_open_probe_reopens_breaker() -> None:
    breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout_seconds=0,
    )

    probe_started = asyncio.Event()
    keep_probe_running = asyncio.Event()

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

    async def cancellable_probe(_: str) -> None:
        probe_started.set()
        await keep_probe_running.wait()

    async def successful_operation(value: str) -> str:
        return f"processed:{value}"

    # Открываем Circuit Breaker.
    with pytest.raises(httpx.HTTPStatusError):
        await breaker.call(failing_operation, "payment-1")

    assert breaker.state == CircuitBreakerState.OPEN

    # После recovery timeout этот вызов становится единственным HALF_OPEN probe.
    probe_task = asyncio.create_task(breaker.call(cancellable_probe, "payment-2"))

    await probe_started.wait()

    assert breaker.state == CircuitBreakerState.HALF_OPEN

    # Имитируем shutdown, client disconnect или другую отмену task.
    probe_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await probe_task

    # Отмена не должна навечно оставлять занятый HALF_OPEN permit.
    assert breaker.state == CircuitBreakerState.OPEN
    assert breaker.opened_at is not None

    # При recovery_timeout=0 следующий probe должен быть разрешён.
    result = await breaker.call(successful_operation, "payment-3")

    assert result == "processed:payment-3"
    assert breaker.state == CircuitBreakerState.CLOSED
    assert breaker.failure_count == 0
    assert breaker.opened_at is None


@pytest.mark.asyncio
async def test_cancelled_regular_call_does_not_count_as_provider_failure() -> None:
    breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout_seconds=30,
    )

    operation_started = asyncio.Event()
    keep_operation_running = asyncio.Event()

    async def cancellable_operation(_: str) -> None:
        operation_started.set()
        await keep_operation_running.wait()

    operation_task = asyncio.create_task(
        breaker.call(cancellable_operation, "payment-1")
    )

    await operation_started.wait()

    operation_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await operation_task

    assert breaker.state == CircuitBreakerState.CLOSED
    assert breaker.failure_count == 0
    assert breaker.opened_at is None
