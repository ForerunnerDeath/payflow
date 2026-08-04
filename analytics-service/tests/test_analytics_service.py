from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from app.models.transaction import Transaction
from app.services.analytics import AnalyticsService
from app.services.analytics_cache import (
    AnalyticsSummary,
    AnalyticsSummaryCache,
    SummaryCacheLookup,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_get_summary_builds_summary_from_currency_rows() -> None:
    session = MagicMock(spec=AsyncSession)

    result = MagicMock()
    result.tuples.return_value.all.return_value = [
        (
            "RUB",
            3,
            Decimal("4500.00"),
            Decimal("1500.00"),
            2,
            1,
        ),
        (
            "USD",
            2,
            Decimal("70.00"),
            Decimal("35.00"),
            2,
            0,
        ),
    ]

    session.execute = AsyncMock(return_value=result)

    service = AnalyticsService(session)

    summary = await service.get_summary()

    session.execute.assert_awaited_once()

    assert summary.total_transactions == 5
    assert summary.completed_transactions == 4
    assert summary.failed_transactions == 1

    assert len(summary.currencies) == 2

    rub_summary = summary.currencies[0]

    assert rub_summary.currency == "RUB"
    assert rub_summary.transaction_count == 3
    assert rub_summary.total_amount == Decimal("4500.00")
    assert rub_summary.average_amount == Decimal("1500.00")

    usd_summary = summary.currencies[1]

    assert usd_summary.currency == "USD"
    assert usd_summary.transaction_count == 2
    assert usd_summary.total_amount == Decimal("70.00")
    assert usd_summary.average_amount == Decimal("35.00")


@pytest.mark.asyncio
async def test_get_summary_applies_currency_and_date_filters() -> None:
    session = MagicMock(spec=AsyncSession)

    result = MagicMock()
    result.tuples.return_value.all.return_value = []
    session.execute = AsyncMock(return_value=result)

    service = AnalyticsService(session)

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

    summary = await service.get_summary(
        currency="RUB",
        date_from=date_from,
        date_to=date_to,
    )

    session.execute.assert_awaited_once()

    statement = session.execute.await_args.args[0]
    compiled = statement.compile(
        dialect=postgresql.dialect(),
    )

    sql = str(compiled)
    params = compiled.params

    assert "transactions.currency =" in sql
    assert "transactions.processed_at >=" in sql
    assert "transactions.processed_at <=" in sql
    assert "GROUP BY transactions.currency" in sql
    assert "ORDER BY transactions.currency" in sql

    assert "RUB" in params.values()
    assert date_from in params.values()
    assert date_to in params.values()

    assert summary.total_transactions == 0
    assert summary.completed_transactions == 0
    assert summary.failed_transactions == 0
    assert summary.currencies == []


@pytest.mark.asyncio
async def test_get_transactions_returns_paginated_result() -> None:
    session = MagicMock(spec=AsyncSession)

    first_transaction = Transaction(
        id=UUID("11111111-1111-1111-1111-111111111111"),
        payment_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
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

    second_transaction = Transaction(
        id=UUID("22222222-2222-2222-2222-222222222222"),
        payment_id=UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        amount=Decimal("700.00"),
        currency="RUB",
        status="failed",
        event_type="payment.failed",
        processed_at=datetime(
            2026,
            8,
            3,
            11,
            0,
            tzinfo=UTC,
        ),
    )

    count_result = MagicMock()
    count_result.scalar_one.return_value = 2

    transactions_result = MagicMock()
    transactions_result.scalars.return_value.all.return_value = [
        first_transaction,
        second_transaction,
    ]

    session.execute = AsyncMock(
        side_effect=[
            count_result,
            transactions_result,
        ]
    )

    service = AnalyticsService(session)

    result = await service.get_transactions(
        limit=20,
        offset=0,
    )

    assert session.execute.await_count == 2

    assert result.total == 2
    assert result.limit == 20
    assert result.offset == 0
    assert len(result.items) == 2

    assert result.items[0].payment_id == (first_transaction.payment_id)
    assert result.items[0].amount == Decimal("1500.50")
    assert result.items[0].status == "completed"

    assert result.items[1].payment_id == (second_transaction.payment_id)
    assert result.items[1].amount == Decimal("700.00")
    assert result.items[1].status == "failed"


@pytest.mark.asyncio
async def test_get_transactions_applies_filters_sorting_and_pagination() -> None:
    session = MagicMock(spec=AsyncSession)

    count_result = MagicMock()
    count_result.scalar_one.return_value = 7

    transactions_result = MagicMock()
    transactions_result.scalars.return_value.all.return_value = []

    session.execute = AsyncMock(
        side_effect=[
            count_result,
            transactions_result,
        ]
    )

    service = AnalyticsService(session)

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

    result = await service.get_transactions(
        status="completed",
        currency="RUB",
        date_from=date_from,
        date_to=date_to,
        limit=20,
        offset=40,
    )

    assert session.execute.await_count == 2

    count_statement = session.execute.await_args_list[0].args[0]
    transactions_statement = session.execute.await_args_list[1].args[0]

    compiled_count = count_statement.compile(
        dialect=postgresql.dialect(),
    )
    compiled_transactions = transactions_statement.compile(
        dialect=postgresql.dialect(),
    )

    count_sql = str(compiled_count)
    transactions_sql = str(compiled_transactions)

    assert "transactions.status =" in count_sql
    assert "transactions.currency =" in count_sql
    assert "transactions.processed_at >=" in count_sql
    assert "transactions.processed_at <=" in count_sql

    assert "transactions.status =" in transactions_sql
    assert "transactions.currency =" in transactions_sql
    assert "transactions.processed_at >=" in transactions_sql
    assert "transactions.processed_at <=" in transactions_sql

    assert (
        "ORDER BY transactions.processed_at DESC, transactions.id DESC"
    ) in transactions_sql
    assert "LIMIT" in transactions_sql
    assert "OFFSET" in transactions_sql

    count_params = compiled_count.params.values()
    transactions_params = compiled_transactions.params.values()

    assert "completed" in count_params
    assert "RUB" in count_params
    assert date_from in count_params
    assert date_to in count_params

    assert "completed" in transactions_params
    assert "RUB" in transactions_params
    assert date_from in transactions_params
    assert date_to in transactions_params
    assert 20 in transactions_params
    assert 40 in transactions_params

    assert result.total == 7
    assert result.limit == 20
    assert result.offset == 40
    assert result.items == []


@pytest.mark.asyncio
async def test_get_transaction_by_payment_id_returns_transaction() -> None:
    session = MagicMock(spec=AsyncSession)

    payment_id = UUID("22222222-2222-2222-2222-222222222222")

    transaction = Transaction(
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

    query_result = MagicMock()
    query_result.scalar_one_or_none.return_value = transaction
    session.execute = AsyncMock(return_value=query_result)

    service = AnalyticsService(session)

    result = await service.get_transaction_by_payment_id(payment_id)

    session.execute.assert_awaited_once()

    assert result is not None
    assert result.payment_id == payment_id
    assert result.amount == Decimal("1500.50")
    assert result.currency == "RUB"
    assert result.status == "completed"


@pytest.mark.asyncio
async def test_get_transaction_by_payment_id_returns_none_when_not_found() -> None:
    session = MagicMock(spec=AsyncSession)

    query_result = MagicMock()
    query_result.scalar_one_or_none.return_value = None
    session.execute = AsyncMock(return_value=query_result)

    service = AnalyticsService(session)

    payment_id = UUID("22222222-2222-2222-2222-222222222222")

    result = await service.get_transaction_by_payment_id(payment_id)

    session.execute.assert_awaited_once()
    assert result is None


@pytest.mark.asyncio
async def test_get_summary_rounds_money_values_to_two_decimal_places() -> None:
    session = MagicMock(spec=AsyncSession)

    result = MagicMock()
    result.tuples.return_value.all.return_value = [
        (
            "RUB",
            3,
            Decimal("100.0000000000000000"),
            Decimal("33.3333333333333333"),
            2,
            1,
        )
    ]

    session.execute = AsyncMock(return_value=result)

    service = AnalyticsService(session)

    summary = await service.get_summary()

    currency_summary = summary.currencies[0]

    assert currency_summary.total_amount == Decimal("100.00")
    assert currency_summary.average_amount == Decimal("33.33")


@pytest.mark.asyncio
async def test_get_summary_returns_cached_result_without_database_query() -> None:
    session = MagicMock(spec=AsyncSession)

    summary = AnalyticsSummary(
        total_transactions=1,
        completed_transactions=1,
        failed_transactions=0,
        currencies=[],
    )

    cache = MagicMock(spec=AnalyticsSummaryCache)
    cache.lookup = AsyncMock(
        return_value=SummaryCacheLookup(
            key="analytics:summary:v3:currency=RUB:from=all:to=all",
            summary=summary,
        )
    )
    cache.store = AsyncMock()

    service = AnalyticsService(session, summary_cache=cache)

    result = await service.get_summary(currency="RUB")

    assert result == summary

    cache.lookup.assert_awaited_once_with(
        currency="RUB",
        date_from=None,
        date_to=None,
    )
    cache.store.assert_not_awaited()
    session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_get_summary_stores_database_result_after_cache_miss() -> None:
    session = MagicMock(spec=AsyncSession)

    database_result = MagicMock()
    database_result.tuples.return_value.all.return_value = []
    session.execute = AsyncMock(return_value=database_result)

    cache_key = "analytics:summary:v3:currency=RUB:from=all:to=all"

    cache = MagicMock(spec=AnalyticsSummaryCache)
    cache.lookup = AsyncMock(
        return_value=SummaryCacheLookup(key=cache_key, summary=None)
    )
    cache.store = AsyncMock()

    service = AnalyticsService(session, summary_cache=cache)

    summary = await service.get_summary(currency="RUB")

    assert summary == AnalyticsSummary(
        total_transactions=0,
        completed_transactions=0,
        failed_transactions=0,
        currencies=[],
    )

    session.execute.assert_awaited_once()

    cache.store.assert_awaited_once_with(key=cache_key, summary=summary)
