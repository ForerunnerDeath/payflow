from datetime import UTC, datetime
from decimal import Decimal
from typing import Literal, Self
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, model_validator

from app.models.payment import PaymentStatus


class PaymentEventPayload(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    event_type: Literal["payment.completed", "payment.failed"]
    payment_id: UUID
    amount: Decimal = Field(gt=Decimal("0.00"), max_digits=18, decimal_places=2)
    currency: str = Field(min_length=3, max_length=3, pattern=r"^[A-Z]{3}$")
    status: Literal[PaymentStatus.COMPLETED, PaymentStatus.FAILED]
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def check_event_type_and_status(self) -> Self:
        if (
            self.event_type == "payment.completed"
            and self.status == PaymentStatus.FAILED
        ) or (
            self.event_type == "payment.failed"
            and self.status == PaymentStatus.COMPLETED
        ):
            raise ValueError("event_type does not match status")
        return self
