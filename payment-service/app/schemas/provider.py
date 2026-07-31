from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field


class ProviderPaymentRequest(BaseModel):
    payment_id: UUID
    amount: Decimal = Field(gt=Decimal("0.00"), max_digits=18, decimal_places=2)
    currency: str = Field(min_length=3, max_length=3, pattern=r"^[A-Z]{3}$")


class ProviderPaymentResponse(BaseModel):
    provider_payment_id: UUID
    status: Literal["approved"]
