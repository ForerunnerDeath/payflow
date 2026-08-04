from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest
from app.api.analytics import get_summary_cache
from app.core.database import get_session
from app.main import app
from app.schemas.analytics import (
    AnalyticsSummary,
    CurrencySummary,
    TransactionListResponse,
    TransactionResponse,
)
from app.services.analytics import AnalyticsService
from app.services.analytics_cache import AnalyticsSummaryCache
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_summary_endpoint_passes_filters_to_service() -> None:
    session = MagicMock(spec=AsyncSession)

    summary_cache = cast(AnalyticsSummaryCache, MagicMock(spec=AnalyticsSummaryCache))

    async def override_get_session() -> AsyncIterator[AsyncSession]:
        yield cast(AsyncSession, session)

    app.dependency_overrides[get_session] = override_get_session

    def override_get_summary_cache() -> AnalyticsSummaryCache:
        return summary_cache

    app.dependency_overrides[get_summary_cache] = override_get_summary_cache

    date_from = datetime(
        2026,
        8,
        1,
        0,
        0,
        tzinfo=UTC,
    )
    date_to = datetime(
        2026,
        8,
        31,
        23,
        59,
        59,
        tzinfo=UTC,
    )

    expected_summary = AnalyticsSummary(
        total_transactions=3,
        completed_transactions=2,
        failed_transactions=1,
        currencies=[
            CurrencySummary(
                currency="RUB",
                transaction_count=3,
                total_amount=Decimal("4500.00"),
                average_amount=Decimal("1500.00"),
            )
        ],
    )

    service = MagicMock(spec=AnalyticsService)
    service.get_summary = AsyncMock(
        return_value=expected_summary,
    )

    try:
        with patch(
            "app.api.analytics.AnalyticsService",
            return_value=service,
        ) as service_class:
            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test",
            ) as client:
                response = await client.get(
                    "/api/v1/analytics/summary",
                    params={
                        "currency": "rub",
                        "date_from": "2026-08-01T00:00:00Z",
                        "date_to": "2026-08-31T23:59:59Z",
                    },
                )

        service_class.assert_called_once_with(session, summary_cache=summary_cache)

        service.get_summary.assert_awaited_once_with(
            currency="RUB",
            date_from=date_from,
            date_to=date_to,
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200

    assert response.json() == {
        "total_transactions": 3,
        "completed_transactions": 2,
        "failed_transactions": 1,
        "currencies": [
            {
                "currency": "RUB",
                "transaction_count": 3,
                "total_amount": "4500.00",
                "average_amount": "1500.00",
            }
        ],
    }


@pytest.mark.asyncio
async def test_summary_endpoint_rejects_invalid_date_range() -> None:
    session = MagicMock(spec=AsyncSession)

    summary_cache = cast(AnalyticsSummaryCache, MagicMock(spec=AnalyticsSummaryCache))

    async def override_get_session() -> AsyncIterator[AsyncSession]:
        yield cast(AsyncSession, session)

    app.dependency_overrides[get_session] = override_get_session

    def override_get_summary_cache() -> AnalyticsSummaryCache:
        return summary_cache

    app.dependency_overrides[get_summary_cache] = override_get_summary_cache

    service = MagicMock(spec=AnalyticsService)
    service.get_summary = AsyncMock()

    try:
        with patch(
            "app.api.analytics.AnalyticsService",
            return_value=service,
        ):
            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test",
            ) as client:
                response = await client.get(
                    "/api/v1/analytics/summary",
                    params={
                        "date_from": "2026-08-31T00:00:00Z",
                        "date_to": "2026-08-01T00:00:00Z",
                    },
                )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    assert response.json() == {
        "detail": "date_from must be before or equal to date_to",
    }

    service.get_summary.assert_not_awaited()


@pytest.mark.asyncio
async def test_summary_endpoint_rejects_invalid_currency() -> None:
    session = MagicMock(spec=AsyncSession)

    summary_cache = cast(AnalyticsSummaryCache, MagicMock(spec=AnalyticsSummaryCache))

    async def override_get_session() -> AsyncIterator[AsyncSession]:
        yield cast(AsyncSession, session)

    app.dependency_overrides[get_session] = override_get_session

    def override_get_summary_cache() -> AnalyticsSummaryCache:
        return summary_cache

    app.dependency_overrides[get_summary_cache] = override_get_summary_cache

    service = MagicMock(spec=AnalyticsService)
    service.get_summary = AsyncMock()

    try:
        with patch(
            "app.api.analytics.AnalyticsService",
            return_value=service,
        ):
            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test",
            ) as client:
                response = await client.get(
                    "/api/v1/analytics/summary",
                    params={
                        "currency": "RUBLE",
                    },
                )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 422
    service.get_summary.assert_not_awaited()


@pytest.mark.asyncio
async def test_transactions_endpoint_passes_filters_and_pagination() -> None:
    session = MagicMock(spec=AsyncSession)

    async def override_get_session() -> AsyncIterator[AsyncSession]:
        yield cast(AsyncSession, session)

    app.dependency_overrides[get_session] = override_get_session

    date_from = datetime(
        2026,
        8,
        1,
        0,
        0,
        tzinfo=UTC,
    )
    date_to = datetime(
        2026,
        8,
        31,
        23,
        59,
        59,
        tzinfo=UTC,
    )

    transaction = TransactionResponse(
        id=UUID("11111111-1111-1111-1111-111111111111"),
        payment_id=UUID("22222222-2222-2222-2222-222222222222"),
        amount=Decimal("1500.50"),
        currency="RUB",
        status="completed",
        event_type="payment.completed",
        processed_at=datetime(
            2026,
            8,
            3,
            12,
            0,
            tzinfo=UTC,
        ),
    )

    expected_result = TransactionListResponse(
        items=[transaction],
        total=21,
        limit=10,
        offset=20,
    )

    service = MagicMock(spec=AnalyticsService)
    service.get_transactions = AsyncMock(
        return_value=expected_result,
    )

    try:
        with patch(
            "app.api.analytics.AnalyticsService",
            return_value=service,
        ) as service_class:
            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test",
            ) as client:
                response = await client.get(
                    "/api/v1/analytics/transactions",
                    params={
                        "status": "completed",
                        "currency": "rub",
                        "date_from": "2026-08-01T00:00:00Z",
                        "date_to": "2026-08-31T23:59:59Z",
                        "limit": 10,
                        "offset": 20,
                    },
                )

        service_class.assert_called_once_with(session)

        service.get_transactions.assert_awaited_once_with(
            status="completed",
            currency="RUB",
            date_from=date_from,
            date_to=date_to,
            limit=10,
            offset=20,
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200

    payload = response.json()

    assert payload["total"] == 21
    assert payload["limit"] == 10
    assert payload["offset"] == 20
    assert len(payload["items"]) == 1

    assert payload["items"][0]["payment_id"] == ("22222222-2222-2222-2222-222222222222")
    assert payload["items"][0]["amount"] == "1500.50"
    assert payload["items"][0]["currency"] == "RUB"
    assert payload["items"][0]["status"] == "completed"


@pytest.mark.asyncio
async def test_transaction_endpoint_returns_transaction() -> None:
    session = MagicMock(spec=AsyncSession)

    async def override_get_session() -> AsyncIterator[AsyncSession]:
        yield cast(AsyncSession, session)

    app.dependency_overrides[get_session] = override_get_session

    payment_id = UUID("22222222-2222-2222-2222-222222222222")

    transaction = TransactionResponse(
        id=UUID("11111111-1111-1111-1111-111111111111"),
        payment_id=payment_id,
        amount=Decimal("1500.50"),
        currency="RUB",
        status="completed",
        event_type="payment.completed",
        processed_at=datetime(
            2026,
            8,
            3,
            12,
            0,
            tzinfo=UTC,
        ),
    )

    service = MagicMock(spec=AnalyticsService)
    service.get_transaction_by_payment_id = AsyncMock(
        return_value=transaction,
    )

    try:
        with patch(
            "app.api.analytics.AnalyticsService",
            return_value=service,
        ) as service_class:
            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test",
            ) as client:
                response = await client.get(
                    f"/api/v1/analytics/transactions/{payment_id}"
                )

        service_class.assert_called_once_with(session)
        service.get_transaction_by_payment_id.assert_awaited_once_with(
            payment_id,
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "id": "11111111-1111-1111-1111-111111111111",
        "payment_id": "22222222-2222-2222-2222-222222222222",
        "amount": "1500.50",
        "currency": "RUB",
        "status": "completed",
        "event_type": "payment.completed",
        "processed_at": "2026-08-03T12:00:00Z",
    }


@pytest.mark.asyncio
async def test_transaction_endpoint_returns_404_when_not_found() -> None:
    session = MagicMock(spec=AsyncSession)

    async def override_get_session() -> AsyncIterator[AsyncSession]:
        yield cast(AsyncSession, session)

    app.dependency_overrides[get_session] = override_get_session

    payment_id = UUID("22222222-2222-2222-2222-222222222222")

    service = MagicMock(spec=AnalyticsService)
    service.get_transaction_by_payment_id = AsyncMock(return_value=None)

    try:
        with patch("app.api.analytics.AnalyticsService", return_value=service):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await client.get(
                    f"/api/v1/analytics/transactions/{payment_id}"
                )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 404
    assert response.json() == {"detail": "Transaction not found"}

    service.get_transaction_by_payment_id.assert_awaited_once_with(payment_id)
