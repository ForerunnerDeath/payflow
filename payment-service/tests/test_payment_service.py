from decimal import Decimal
from typing import cast
from unittest.mock import AsyncMock, call
from uuid import uuid4

import httpx
import pytest
from app.clients.circuit_breaker import CircuitBreaker
from app.clients.payment_provider import PaymentProviderClient
from app.models.outbox_event import OutboxEvent
from app.models.payment import Payment, PaymentStatus
from app.repositories.payment import PaymentRepository
from app.schemas.payment import PaymentCreate
from app.schemas.provider import ProviderPaymentRequest, ProviderPaymentResponse
from app.services.payment import PaymentService
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession


def make_payment_service(session: AsyncSession) -> tuple[PaymentService, AsyncMock]:
    provider_client_mock = AsyncMock(spec=PaymentProviderClient)
    provider_client = cast(PaymentProviderClient, provider_client_mock)

    circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout_seconds=30)

    service = PaymentService(
        session=session,
        provider_client=provider_client,
        circuit_breaker=circuit_breaker,
    )

    return service, provider_client_mock


@pytest.mark.asyncio
async def test_create_payment_returns_existing_payment() -> None:
    session = AsyncMock(spec=AsyncSession)
    repository = AsyncMock(spec=PaymentRepository)
    service, _ = make_payment_service(session)
    service._repository = repository  # pyright: ignore[reportPrivateUsage]

    data = PaymentCreate(
        amount=Decimal("1500.50"),
        currency="RUB",
        description="Оплата заказа",
        idempotency_key="order-123",
        customer_id=uuid4(),
    )

    existing_payment = Payment(
        id=uuid4(),
        amount=data.amount,
        currency=data.currency,
        status=PaymentStatus.PENDING,
        description=data.description,
        idempotency_key=data.idempotency_key,
        customer_id=data.customer_id,
    )

    repository.get_by_idempotency_key.return_value = existing_payment
    result = await service.create_payment(data)
    assert result is existing_payment

    repository.get_by_idempotency_key.assert_awaited_once_with(data.idempotency_key)
    repository.add.assert_not_awaited()
    session.commit.assert_not_awaited()
    session.rollback.assert_not_awaited()
    session.refresh.assert_not_awaited()
    session.add.assert_not_called()


@pytest.mark.asyncio
async def test_create_payment_commits_new_payment() -> None:
    session = AsyncMock(spec=AsyncSession)
    repository = AsyncMock(spec=PaymentRepository)

    service, provider_client_mock = make_payment_service(session)
    service._repository = repository  # pyright: ignore[reportPrivateUsage]

    data = PaymentCreate(
        amount=Decimal("1500.50"),
        currency="RUB",
        description="Оплата заказа",
        idempotency_key="order-123",
        customer_id=uuid4(),
    )

    repository.get_by_idempotency_key.return_value = None

    payment_id = uuid4()
    provider_payment_id = uuid4()

    def return_payment(payment: Payment) -> Payment:
        payment.id = payment_id
        return payment

    repository.add.side_effect = return_payment

    provider_client_mock.process_payment.return_value = ProviderPaymentResponse(
        provider_payment_id=provider_payment_id,
        status="approved",
    )

    result = await service.create_payment(data)

    repository.get_by_idempotency_key.assert_awaited_once_with(data.idempotency_key)
    repository.add.assert_awaited_once()

    assert session.commit.await_count == 3
    assert session.refresh.await_count == 2
    session.refresh.assert_awaited_with(result)
    session.rollback.assert_not_awaited()

    provider_client_mock.process_payment.assert_awaited_once_with(
        ProviderPaymentRequest(
            payment_id=payment_id,
            amount=data.amount,
            currency=data.currency,
        )
    )

    assert result.id == payment_id
    assert result.amount == data.amount
    assert result.currency == data.currency
    assert result.idempotency_key == data.idempotency_key
    assert result.customer_id == data.customer_id
    assert result.status == PaymentStatus.COMPLETED
    assert result.provider_payment_id == str(provider_payment_id)
    assert result.completed_at is not None
    assert result.failure_reason is None

    session.add.assert_called_once()

    outbox_event = cast(OutboxEvent, session.add.call_args.args[0])

    assert outbox_event.event_type == "payment.completed"
    assert outbox_event.id is not None
    assert outbox_event.payload["event_id"] == str(outbox_event.id)
    assert outbox_event.payload["payment_id"] == str(payment_id)
    assert outbox_event.payload["amount"] == "1500.50"
    assert outbox_event.payload["currency"] == "RUB"
    assert outbox_event.payload["status"] == "completed"
    assert isinstance(outbox_event.payload["timestamp"], str)


@pytest.mark.asyncio
async def test_create_payment_rolls_back_on_unexpected_error() -> None:
    session = AsyncMock(spec=AsyncSession)
    repository = AsyncMock(spec=PaymentRepository)
    service, _ = make_payment_service(session)
    service._repository = repository  # pyright: ignore[reportPrivateUsage]

    data = PaymentCreate(
        amount=Decimal("1500.50"),
        currency="RUB",
        description="Оплата заказа",
        idempotency_key="order-123",
        customer_id=uuid4(),
    )

    repository.get_by_idempotency_key.return_value = None
    repository.add.side_effect = RuntimeError("Database error")

    with pytest.raises(RuntimeError, match="Database error"):
        await service.create_payment(data)

    repository.add.assert_awaited_once()
    session.rollback.assert_awaited_once()
    session.commit.assert_not_awaited()
    session.refresh.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_payment_returns_existing_after_integrity_error() -> None:
    session = AsyncMock(spec=AsyncSession)
    repository = AsyncMock(spec=PaymentRepository)
    service, _ = make_payment_service(session)
    service._repository = repository  # pyright: ignore[reportPrivateUsage]

    data = PaymentCreate(
        amount=Decimal("1500.50"),
        currency="RUB",
        description="Оплата заказа",
        idempotency_key="order-123",
        customer_id=uuid4(),
    )

    existing_payment = Payment(
        id=uuid4(),
        amount=data.amount,
        currency=data.currency,
        status=PaymentStatus.PENDING,
        description=data.description,
        idempotency_key=data.idempotency_key,
        customer_id=data.customer_id,
    )

    repository.get_by_idempotency_key.side_effect = [
        None,
        existing_payment,
    ]

    integrity_error = IntegrityError(
        "INSERT INTO payments ...",
        {},
        Exception("duplicate key"),
    )

    repository.add.side_effect = integrity_error
    result = await service.create_payment(data)

    assert result is existing_payment
    assert repository.get_by_idempotency_key.await_count == 2
    repository.get_by_idempotency_key.assert_has_awaits(
        [
            call(data.idempotency_key),
            call(data.idempotency_key),
        ]
    )
    repository.add.assert_awaited_once()
    session.rollback.assert_awaited_once()
    session.commit.assert_not_awaited()
    session.refresh.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_payment_returns_existing_payment() -> None:
    session = AsyncMock(spec=AsyncSession)
    repository = AsyncMock(spec=PaymentRepository)
    service, _ = make_payment_service(session)
    service._repository = repository  # pyright: ignore[reportPrivateUsage]

    data = PaymentCreate(
        amount=Decimal("1500.50"),
        currency="RUB",
        description="Оплата заказа",
        idempotency_key="order-123",
        customer_id=uuid4(),
    )

    existing_payment = Payment(
        id=uuid4(),
        amount=data.amount,
        currency=data.currency,
        status=PaymentStatus.PENDING,
        description=data.description,
        idempotency_key=data.idempotency_key,
        customer_id=data.customer_id,
    )
    repository.get_by_id.return_value = existing_payment
    result = await service.get_payment(existing_payment.id)

    assert result is existing_payment
    repository.get_by_id.assert_awaited_once_with(existing_payment.id)


@pytest.mark.asyncio
async def test_create_payment_marks_failed_on_provider_error() -> None:
    session = AsyncMock(spec=AsyncSession)
    repository = AsyncMock(spec=PaymentRepository)

    service, provider_client_mock = make_payment_service(session)
    service._repository = repository  # pyright: ignore[reportPrivateUsage]

    data = PaymentCreate(
        amount=Decimal("1500.50"),
        currency="RUB",
        description="Оплата заказа",
        idempotency_key="order-123",
        customer_id=uuid4(),
    )

    repository.get_by_idempotency_key.return_value = None

    payment_id = uuid4()

    def return_payment(payment: Payment) -> Payment:
        payment.id = payment_id
        return payment

    repository.add.side_effect = return_payment

    provider_http_request = httpx.Request(
        method="POST",
        url="http://provider.test/process-payment",
    )

    provider_client_mock.process_payment.side_effect = httpx.ReadTimeout(
        "Provider timeout",
        request=provider_http_request,
    )

    result = await service.create_payment(data)

    provider_client_mock.process_payment.assert_awaited_once_with(
        ProviderPaymentRequest(
            payment_id=payment_id,
            amount=data.amount,
            currency=data.currency,
        )
    )

    assert result.status == PaymentStatus.FAILED
    assert result.failure_reason == "Provider timeout"
    assert result.provider_payment_id is None
    assert result.completed_at is None

    assert session.commit.await_count == 3
    assert session.refresh.await_count == 2
    session.rollback.assert_not_awaited()

    session.add.assert_called_once()

    outbox_event = cast(OutboxEvent, session.add.call_args.args[0])

    assert outbox_event.event_type == "payment.failed"
    assert outbox_event.id is not None
    assert outbox_event.payload["event_id"] == str(outbox_event.id)
    assert outbox_event.payload["payment_id"] == str(payment_id)
    assert outbox_event.payload["amount"] == "1500.50"
    assert outbox_event.payload["currency"] == "RUB"
    assert outbox_event.payload["status"] == "failed"
    assert isinstance(outbox_event.payload["timestamp"], str)
