from collections.abc import Sequence
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import cast
from uuid import UUID

from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from app.models.transaction import Transaction
from app.schemas.analytics import (
    AnalyticsSummary,
    CurrencySummary,
    TransactionListResponse,
    TransactionResponse,
)

SummaryRow = tuple[str, int, Decimal, Decimal, int, int]
MONEY_QUANTUM = Decimal("0.01")


class AnalyticsService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @staticmethod
    def _build_filters(
        *,
        status: str | None = None,
        currency: str | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[ColumnElement[bool]]:
        filters: list[ColumnElement[bool]] = []

        if status is not None:
            filters.append(Transaction.status == status)

        if currency is not None:
            filters.append(Transaction.currency == currency)

        if date_from is not None:
            filters.append(Transaction.processed_at >= date_from)

        if date_to is not None:
            filters.append(Transaction.processed_at <= date_to)

        return filters

    async def get_summary(
        self,
        *,
        currency: str | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> AnalyticsSummary:
        filters = self._build_filters(
            currency=currency,
            date_from=date_from,
            date_to=date_to,
        )

        statement = (
            select(
                Transaction.currency,
                func.count(Transaction.id),
                func.sum(Transaction.amount),
                func.avg(Transaction.amount),
                func.sum(
                    case(
                        (Transaction.status == "completed", 1),
                        else_=0,
                    )
                ),
                func.sum(
                    case(
                        (Transaction.status == "failed", 1),
                        else_=0,
                    )
                ),
            )
            .where(*filters)
            .group_by(Transaction.currency)
            .order_by(Transaction.currency)
        )

        result = await self._session.execute(statement)

        rows = cast(Sequence[SummaryRow], result.tuples().all())

        currencies: list[CurrencySummary] = []
        total_transactions = 0
        completed_transactions = 0
        failed_transactions = 0

        for (
            row_currency,
            transaction_count,
            total_amount,
            average_amount,
            completed_count,
            failed_count,
        ) in rows:
            currencies.append(
                CurrencySummary(
                    currency=row_currency,
                    transaction_count=transaction_count,
                    total_amount=total_amount.quantize(
                        MONEY_QUANTUM, rounding=ROUND_HALF_UP
                    ),
                    average_amount=average_amount.quantize(
                        MONEY_QUANTUM, rounding=ROUND_HALF_UP
                    ),
                )
            )

            total_transactions += transaction_count
            completed_transactions += completed_count
            failed_transactions += failed_count

        return AnalyticsSummary(
            total_transactions=total_transactions,
            completed_transactions=completed_transactions,
            failed_transactions=failed_transactions,
            currencies=currencies,
        )

    async def get_transactions(
        self,
        *,
        status: str | None = None,
        currency: str | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        limit: int,
        offset: int,
    ) -> TransactionListResponse:
        filters = self._build_filters(
            status=status,
            currency=currency,
            date_from=date_from,
            date_to=date_to,
        )

        count_statement = select(func.count(Transaction.id)).where(*filters)

        transactions_statement = (
            select(Transaction)
            .where(*filters)
            .order_by(
                Transaction.processed_at.desc(),
                Transaction.id.desc(),
            )
            .limit(limit)
            .offset(offset)
        )

        count_result = await self._session.execute(count_statement)
        total = count_result.scalar_one()

        transactions_result = await self._session.execute(transactions_statement)
        transactions = transactions_result.scalars().all()

        return TransactionListResponse(
            items=[
                TransactionResponse.model_validate(transaction)
                for transaction in transactions
            ],
            total=total,
            limit=limit,
            offset=offset,
        )

    async def get_transaction_by_payment_id(
        self, payment_id: UUID
    ) -> TransactionResponse | None:
        statement = select(Transaction).where(
            Transaction.payment_id == payment_id,
        )

        result = await self._session.execute(statement)
        transaction = result.scalar_one_or_none()

        if transaction is None:
            return None

        return TransactionResponse.model_validate(transaction)
