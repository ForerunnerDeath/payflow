from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.models.payment import PaymentStatus


class PaymentCreate(BaseModel):
    amount: Decimal = Field(gt=Decimal("0.00"), max_digits=18, decimal_places=2)
    currency: str = Field(min_length=3, max_length=3, pattern=r"^[A-Z]{3}$")
    description: str | None = None
    idempotency_key: str = Field(min_length=1, max_length=255)
    customer_id: UUID

    @field_validator("currency", mode="before")
    @classmethod
    def clean_currency(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip().upper()
        return value

    @field_validator("idempotency_key", mode="before")
    @classmethod
    def clean_idempotency_key(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip()
        return value


class PaymentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    amount: Decimal
    currency: str
    status: PaymentStatus
    description: str | None = None
    created_at: datetime
    updated_at: datetime
    idempotency_key: str
    customer_id: UUID
    provider_payment_id: str | None = None
    failure_reason: str | None = None
    completed_at: datetime | None = None
