from datetime import UTC, datetime
from decimal import Decimal
from typing import cast
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from app.schemas.event import PaymentEvent
from app.services.payment_event_processor import PaymentEventProcessor
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


@pytest.mark.asyncio
async def test_process_upserts_transaction_for_new_event() -> None:
    session = MagicMock(spec=AsyncSession)

    session_context = MagicMock()
    session_context.__aenter__ = AsyncMock(return_value=session)
    session_context.__aexit__ = AsyncMock(return_value=None)

    transaction_context = MagicMock()
    transaction_context.__aenter__ = AsyncMock(return_value=None)
    transaction_context.__aexit__ = AsyncMock(return_value=None)

    session.begin.return_value = transaction_context

    session_factory_mock = MagicMock(return_value=session_context)

    processor = PaymentEventProcessor(
        cast(async_sessionmaker[AsyncSession], session_factory_mock)
    )

    event = PaymentEvent(
        event_id=UUID("11111111-1111-1111-1111-111111111111"),
        event_type="payment.completed",
        payment_id=UUID("22222222-2222-2222-2222-222222222222"),
        amount=Decimal("1500.50"),
        currency="RUB",
        status="completed",
        timestamp=datetime(2026, 8, 3, 10, 0, tzinfo=UTC),
    )

    execute_result = MagicMock()
    execute_result.scalar_one_or_none.return_value = event.event_id
    session.execute = AsyncMock(return_value=execute_result)

    processed = await processor.process(event)

    session_factory_mock.assert_called_once_with()
    session.begin.assert_called_once_with()

    assert processed is True
    assert session.execute.await_count == 2
    session.add.assert_not_called()

    transaction_statement = session.execute.await_args_list[1].args[0]
    compiled_statement = str(transaction_statement)

    assert "ON CONFLICT (payment_id) DO UPDATE" in compiled_statement
    assert "amount" in compiled_statement
    assert "currency" in compiled_statement
    assert "status" in compiled_statement
    assert "event_type" in compiled_statement


@pytest.mark.asyncio
async def test_process_skips_transaction_for_duplicate_event() -> None:
    session = MagicMock(spec=AsyncSession)

    session_context = MagicMock()
    session_context.__aenter__ = AsyncMock(return_value=session)
    session_context.__aexit__ = AsyncMock(return_value=None)

    transaction_context = MagicMock()
    transaction_context.__aenter__ = AsyncMock(return_value=None)
    transaction_context.__aexit__ = AsyncMock(return_value=None)

    session.begin.return_value = transaction_context

    execute_result = MagicMock()
    execute_result.scalar_one_or_none.return_value = None
    session.execute = AsyncMock(return_value=execute_result)

    session_factory_mock = MagicMock(return_value=session_context)

    processor = PaymentEventProcessor(
        cast(async_sessionmaker[AsyncSession], session_factory_mock)
    )

    event = PaymentEvent(
        event_id=UUID("11111111-1111-1111-1111-111111111111"),
        event_type="payment.completed",
        payment_id=UUID("22222222-2222-2222-2222-222222222222"),
        amount=Decimal("1500.50"),
        currency="RUB",
        status="completed",
        timestamp=datetime(2026, 8, 3, 10, 0, tzinfo=UTC),
    )

    processed = await processor.process(event)

    assert processed is False
    session.execute.assert_awaited_once()
    session.add.assert_not_called()
