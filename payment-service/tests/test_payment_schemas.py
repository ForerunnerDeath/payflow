from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from typing import TypedDict
from uuid import UUID, uuid4

import pytest
from app.models.payment import PaymentStatus
from app.schemas.payment import PaymentCreate, PaymentResponse
from pydantic import ValidationError


class PaymentCreateData(TypedDict):
    amount: Decimal
    currency: str
    description: str
    idempotency_key: str
    customer_id: UUID


def test_payment_create_schema() -> None:
    valid_data: PaymentCreateData = {
        "amount": Decimal("1500.50"),
        "currency": "  rub ",
        "description": "Оплата заказа",
        "idempotency_key": " order-123 ",
        "customer_id": uuid4(),
    }
    payment_create = PaymentCreate(**valid_data)
    assert payment_create.amount == Decimal("1500.50")
    assert payment_create.currency == "RUB"
    assert payment_create.description == "Оплата заказа"
    assert payment_create.idempotency_key == "order-123"
    assert payment_create.customer_id == valid_data["customer_id"]


@pytest.mark.parametrize(
    "invalid_data",
    [
        {
            "amount": "0",
            "currency": "USD",
            "idempotency_key": "key",
            "customer_id": uuid4(),
        },
        {
            "amount": "-100",
            "currency": "USD",
            "idempotency_key": "key",
            "customer_id": uuid4(),
        },
        {
            "amount": "100",
            "currency": "USDA",
            "idempotency_key": "key",
            "customer_id": uuid4(),
        },
        {
            "amount": "10.001",
            "currency": "usd",
            "idempotency_key": "key",
            "customer_id": uuid4(),
        },
        {
            "amount": "100",
            "currency": "usd",
            "idempotency_key": "k" * 256,
            "customer_id": uuid4(),
        },
        {
            "amount": "100",
            "currency": "usd",
            "idempotency_key": "",
            "customer_id": uuid4(),
        },
        {
            "amount": "100",
            "currency": "usd",
            "idempotency_key": "key",
            "customer_id": None,
        },
    ],
)
def test_payment_create_schema_invalid_data(invalid_data: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(invalid_data)


def test_payment_response_from_attributes() -> None:
    payment_data = SimpleNamespace(
        id=uuid4(),
        amount=Decimal("1500.50"),
        currency="USD",
        status=PaymentStatus.PENDING,
        description="Оплата заказа",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        idempotency_key="order-123",
        customer_id=uuid4(),
        provider_payment_id=None,
        failure_reason=None,
        completed_at=None,
    )
    payment_response = PaymentResponse.model_validate(payment_data)
    assert payment_response.id == payment_data.id
    assert payment_response.amount == payment_data.amount
    assert payment_response.currency == payment_data.currency
    assert payment_response.status == payment_data.status
    assert payment_response.description == payment_data.description
    assert payment_response.created_at == payment_data.created_at
    assert payment_response.updated_at == payment_data.updated_at
    assert payment_response.idempotency_key == payment_data.idempotency_key
    assert payment_response.customer_id == payment_data.customer_id
    assert payment_response.provider_payment_id == payment_data.provider_payment_id
    assert payment_response.failure_reason == payment_data.failure_reason
    assert payment_response.completed_at == payment_data.completed_at
