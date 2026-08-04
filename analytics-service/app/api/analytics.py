from datetime import datetime
from typing import Annotated, Literal, cast
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_session
from app.schemas.analytics import (
    AnalyticsSummary,
    TransactionListResponse,
    TransactionResponse,
)
from app.services.analytics import AnalyticsService
from app.services.analytics_cache import AnalyticsSummaryCache

router = APIRouter(prefix="/api/v1/analytics", tags=["analytics"])

SessionDependency = Annotated[AsyncSession, Depends(get_session)]


def get_summary_cache(request: Request) -> AnalyticsSummaryCache:
    return cast(AnalyticsSummaryCache, request.app.state.analytics_summary_cache)


SummaryCacheDependency = Annotated[AnalyticsSummaryCache, Depends(get_summary_cache)]


CurrencyQuery = Annotated[
    str | None,
    Query(min_length=3, max_length=3, pattern=r"^[A-Za-z]{3}$"),
]

StatusQuery = Annotated[Literal["completed", "failed"] | None, Query()]

LimitQuery = Annotated[int, Query(ge=1, le=100)]

OffsetQuery = Annotated[int, Query(ge=0)]


def _validate_date_range(date_from: datetime | None, date_to: datetime | None) -> None:
    if date_from is not None and date_to is not None and date_from > date_to:
        raise HTTPException(
            status_code=422, detail="date_from must be before or equal to date_to"
        )


@router.get("/summary", response_model=AnalyticsSummary)
async def get_analytics_summary(
    session: SessionDependency,
    summary_cache: SummaryCacheDependency,
    currency: CurrencyQuery = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
) -> AnalyticsSummary:
    _validate_date_range(date_from, date_to)

    service = AnalyticsService(session, summary_cache=summary_cache)

    return await service.get_summary(
        currency=currency.upper() if currency is not None else None,
        date_from=date_from,
        date_to=date_to,
    )


@router.get("/transactions", response_model=TransactionListResponse)
async def get_analytics_transactions(
    session: SessionDependency,
    status: StatusQuery = None,
    currency: CurrencyQuery = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    limit: LimitQuery = 20,
    offset: OffsetQuery = 0,
) -> TransactionListResponse:
    _validate_date_range(date_from, date_to)

    service = AnalyticsService(session)

    return await service.get_transactions(
        status=status,
        currency=currency.upper() if currency is not None else None,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
        offset=offset,
    )


@router.get("/transactions/{payment_id}", response_model=TransactionResponse)
async def get_analytics_transaction(
    payment_id: UUID,
    session: SessionDependency,
) -> TransactionResponse:
    service = AnalyticsService(session)

    transaction = await service.get_transaction_by_payment_id(payment_id)

    if transaction is None:
        raise HTTPException(status_code=404, detail="Transaction not found")

    return transaction
