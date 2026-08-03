from decimal import Decimal
from uuid import UUID

import pytest
from app.schemas.event import PaymentEvent
from pydantic import ValidationError


def make_event_data() -> dict[str, object]:
    return {
        "event_id": "11111111-1111-1111-1111-111111111111",
        "event_type": "payment.completed",
        "payment_id": "22222222-2222-2222-2222-222222222222",
        "amount": "1500.50",
        "currency": "RUB",
        "status": "completed",
        "timestamp": "2026-08-03T10:00:00Z",
    }


def test_payment_event_validates_completed_event() -> None:
    event = PaymentEvent.model_validate(make_event_data())

    assert event.event_id == UUID("11111111-1111-1111-1111-111111111111")
    assert event.payment_id == UUID("22222222-2222-2222-2222-222222222222")
    assert event.amount == Decimal("1500.50")
    assert event.currency == "RUB"
    assert event.status == "completed"


def test_payment_event_rejects_mismatched_event_type_and_status() -> None:
    data = make_event_data()
    data["status"] = "failed"

    with pytest.raises(ValidationError, match="event_type does not match status"):
        PaymentEvent.model_validate(data)


def test_payment_event_requires_event_id() -> None:
    data = make_event_data()
    del data["event_id"]

    with pytest.raises(ValidationError) as exc_info:
        PaymentEvent.model_validate(data)

    assert "event_id" in str(exc_info.value)
