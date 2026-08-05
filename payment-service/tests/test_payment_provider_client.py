from decimal import Decimal
from unittest.mock import Mock, call
from uuid import uuid4

import app.clients.payment_provider as payment_provider_module
import httpx
import pytest
from app.clients.payment_provider import PaymentProviderClient
from app.schemas.provider import ProviderPaymentRequest
from pydantic import ValidationError


@pytest.mark.asyncio
async def test_retries_on_5xx_and_returns_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger_mock = Mock()
    monkeypatch.setattr(payment_provider_module, "logger", logger_mock)
    attempts = 0
    provider_payment_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1

        if attempts < 3:
            return httpx.Response(status_code=500, request=request)

        return httpx.Response(
            status_code=200,
            request=request,
            json={
                "provider_payment_id": str(provider_payment_id),
                "status": "approved",
            },
        )

    transport = httpx.MockTransport(handler)

    async with httpx.AsyncClient(
        transport=transport, base_url="http://provider.test"
    ) as http_client:
        provider_client = PaymentProviderClient(
            client=http_client,
            max_attempts=3,
            retry_base_delay_seconds=0,
        )

        request_data = ProviderPaymentRequest(
            payment_id=uuid4(),
            amount=Decimal("500.15"),
            currency="RUB",
        )

        result = await provider_client.process_payment(request_data)

    assert attempts == 3
    assert result.status == "approved"
    assert result.provider_payment_id == provider_payment_id

    assert logger_mock.warning.call_args_list == [
        call(
            "payment_provider_retry_scheduled",
            attempt=1,
            next_attempt=2,
            max_attempts=3,
            delay_seconds=0.0,
            error_type="HTTPStatusError",
            status_code=500,
        ),
        call(
            "payment_provider_retry_scheduled",
            attempt=2,
            next_attempt=3,
            max_attempts=3,
            delay_seconds=0.0,
            error_type="HTTPStatusError",
            status_code=500,
        ),
    ]

    logger_mock.error.assert_not_called()


@pytest.mark.asyncio
async def test_does_not_retry_on_4xx() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1

        return httpx.Response(
            status_code=400,
            request=request,
        )

    transport = httpx.MockTransport(handler)

    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://provider.test",
    ) as http_client:
        provider_client = PaymentProviderClient(
            client=http_client,
            max_attempts=3,
            retry_base_delay_seconds=0,
        )

        request_data = ProviderPaymentRequest(
            payment_id=uuid4(),
            amount=Decimal("500.15"),
            currency="RUB",
        )

        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            await provider_client.process_payment(request_data)

    assert attempts == 1
    assert exc_info.value.response.status_code == 400


@pytest.mark.asyncio
async def test_raises_after_all_5xx_attempts_are_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger_mock = Mock()
    monkeypatch.setattr(payment_provider_module, "logger", logger_mock)
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1

        return httpx.Response(
            status_code=500,
            request=request,
        )

    transport = httpx.MockTransport(handler)

    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://provider.test",
    ) as http_client:
        provider_client = PaymentProviderClient(
            client=http_client,
            max_attempts=3,
            retry_base_delay_seconds=0,
        )

        request_data = ProviderPaymentRequest(
            payment_id=uuid4(),
            amount=Decimal("500.15"),
            currency="RUB",
        )

        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            await provider_client.process_payment(request_data)

    assert attempts == 3
    assert exc_info.value.response.status_code == 500
    logger_mock.error.assert_called_once_with(
        "payment_provider_retry_exhausted",
        attempt=3,
        max_attempts=3,
        error_type="HTTPStatusError",
        status_code=500,
    )


@pytest.mark.asyncio
async def test_retries_on_timeout_and_returns_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger_mock = Mock()
    monkeypatch.setattr(payment_provider_module, "logger", logger_mock)
    attempts = 0
    provider_payment_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1

        if attempts < 3:
            raise httpx.ReadTimeout(
                "Mock provider timeout",
                request=request,
            )

        return httpx.Response(
            status_code=200,
            request=request,
            json={
                "provider_payment_id": str(provider_payment_id),
                "status": "approved",
            },
        )

    transport = httpx.MockTransport(handler)

    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://provider.test",
    ) as http_client:
        provider_client = PaymentProviderClient(
            client=http_client,
            max_attempts=3,
            retry_base_delay_seconds=0,
        )

        request_data = ProviderPaymentRequest(
            payment_id=uuid4(),
            amount=Decimal("500.15"),
            currency="RUB",
        )

        result = await provider_client.process_payment(request_data)

    assert attempts == 3
    assert result.status == "approved"
    assert result.provider_payment_id == provider_payment_id

    assert logger_mock.warning.call_args_list == [
        call(
            "payment_provider_retry_scheduled",
            attempt=1,
            next_attempt=2,
            max_attempts=3,
            delay_seconds=0.0,
            error_type="ReadTimeout",
            status_code=None,
        ),
        call(
            "payment_provider_retry_scheduled",
            attempt=2,
            next_attempt=3,
            max_attempts=3,
            delay_seconds=0.0,
            error_type="ReadTimeout",
            status_code=None,
        ),
    ]

    logger_mock.error.assert_not_called()


@pytest.mark.asyncio
async def test_raises_after_all_timeout_attempts_are_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger_mock = Mock()
    monkeypatch.setattr(payment_provider_module, "logger", logger_mock)
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1

        raise httpx.ReadTimeout(
            "Mock provider timeout",
            request=request,
        )

    transport = httpx.MockTransport(handler)

    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://provider.test",
    ) as http_client:
        provider_client = PaymentProviderClient(
            client=http_client,
            max_attempts=3,
            retry_base_delay_seconds=0,
        )

        request_data = ProviderPaymentRequest(
            payment_id=uuid4(),
            amount=Decimal("500.15"),
            currency="RUB",
        )

        with pytest.raises(httpx.ReadTimeout):
            await provider_client.process_payment(request_data)

    assert attempts == 3
    logger_mock.error.assert_called_once_with(
        "payment_provider_retry_exhausted",
        attempt=3,
        max_attempts=3,
        error_type="ReadTimeout",
        status_code=None,
    )


@pytest.mark.asyncio
async def test_does_not_retry_invalid_provider_response() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1

        return httpx.Response(
            status_code=200,
            request=request,
            json={
                "provider_payment_id": str(uuid4()),
                "status": "declined",
            },
        )

    transport = httpx.MockTransport(handler)

    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://provider.test",
    ) as http_client:
        provider_client = PaymentProviderClient(
            client=http_client,
            max_attempts=3,
            retry_base_delay_seconds=0,
        )

        request_data = ProviderPaymentRequest(
            payment_id=uuid4(),
            amount=Decimal("500.15"),
            currency="RUB",
        )

        with pytest.raises(ValidationError):
            await provider_client.process_payment(request_data)

    assert attempts == 1


@pytest.mark.asyncio
async def test_sends_same_idempotency_key_on_every_retry() -> None:
    attempts = 0
    received_idempotency_keys: list[str] = []

    payment_id = uuid4()
    provider_payment_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1

        received_idempotency_keys.append(request.headers["Idempotency-Key"])

        if attempts < 3:
            return httpx.Response(
                status_code=500,
                request=request,
            )

        return httpx.Response(
            status_code=200,
            request=request,
            json={
                "provider_payment_id": str(provider_payment_id),
                "status": "approved",
            },
        )

    transport = httpx.MockTransport(handler)

    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://provider.test",
    ) as http_client:
        provider_client = PaymentProviderClient(
            client=http_client,
            max_attempts=3,
            retry_base_delay_seconds=0,
        )

        request_data = ProviderPaymentRequest(
            payment_id=payment_id,
            amount=Decimal("500.15"),
            currency="RUB",
        )

        result = await provider_client.process_payment(request_data)

    assert attempts == 3
    assert result.provider_payment_id == provider_payment_id
    assert received_idempotency_keys == [
        str(payment_id),
        str(payment_id),
        str(payment_id),
    ]
