from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from app.models.payment import PaymentStatus
from app.schemas.event import PaymentEventPayload
from pydantic import ValidationError


def test_payment_completed_event_payload_generates_metadata() -> None:
    payment_id = uuid4()

    event = PaymentEventPayload(
        event_type="payment.completed",
        payment_id=payment_id,
        amount=Decimal("2500.75"),
        currency="RUB",
        status=PaymentStatus.COMPLETED,
    )

    assert isinstance(event.event_id, UUID)
    assert event.payment_id == payment_id
    assert event.event_type == "payment.completed"
    assert event.status == PaymentStatus.COMPLETED
    assert event.timestamp.utcoffset() is not None


def test_event_type_does_not_match_status() -> None:
    payment_id = uuid4()

    with pytest.raises(ValidationError) as exc:
        PaymentEventPayload(
            event_type="payment.completed",
            payment_id=payment_id,
            amount=Decimal("2500.75"),
            currency="RUB",
            status=PaymentStatus.FAILED,
        )

    assert "event_type does not match status" in str(exc.value)
