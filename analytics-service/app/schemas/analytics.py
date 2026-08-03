from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class TransactionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    payment_id: UUID
    amount: Decimal
    currency: str
    status: str
    event_type: str
    processed_at: datetime


class CurrencySummary(BaseModel):
    currency: str
    transaction_count: int
    total_amount: Decimal
    average_amount: Decimal


class AnalyticsSummary(BaseModel):
    total_transactions: int
    completed_transactions: int
    failed_transactions: int
    currencies: list[CurrencySummary]
