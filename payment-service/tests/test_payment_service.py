from decimal import Decimal
from unittest.mock import AsyncMock, call
from uuid import uuid4

import pytest
from app.models.payment import Payment, PaymentStatus
from app.repositories.payment import PaymentRepository
from app.schemas.payment import PaymentCreate
from app.services.payment import PaymentService
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_create_payment_returns_existing_payment() -> None:
    session = AsyncMock(spec=AsyncSession)
    repository = AsyncMock(spec=PaymentRepository)
    service = PaymentService(session)
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


@pytest.mark.asyncio
async def test_create_payment_commits_new_payment() -> None:
    session = AsyncMock(spec=AsyncSession)
    repository = AsyncMock(spec=PaymentRepository)
    service = PaymentService(session)
    service._repository = repository  # pyright: ignore[reportPrivateUsage]

    data = PaymentCreate(
        amount=Decimal("1500.50"),
        currency="RUB",
        description="Оплата заказа",
        idempotency_key="order-123",
        customer_id=uuid4(),
    )

    repository.get_by_idempotency_key.return_value = None

    def return_payment(payment: Payment) -> Payment:
        return payment

    repository.add.side_effect = return_payment
    result = await service.create_payment(data)

    repository.get_by_idempotency_key.assert_awaited_once_with(data.idempotency_key)
    repository.add.assert_awaited_once()
    session.commit.assert_awaited_once()
    session.refresh.assert_awaited_once_with(result)
    session.rollback.assert_not_awaited()

    assert result.amount == data.amount
    assert result.currency == data.currency
    assert result.idempotency_key == data.idempotency_key
    assert result.customer_id == data.customer_id


@pytest.mark.asyncio
async def test_create_payment_rolls_back_on_unexpected_error() -> None:
    session = AsyncMock(spec=AsyncSession)
    repository = AsyncMock(spec=PaymentRepository)
    service = PaymentService(session)
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
    service = PaymentService(session)
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
    service = PaymentService(session)
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
