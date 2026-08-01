from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from app.models.outbox_event import OutboxEvent
from app.repositories.outbox import OutboxRepository
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_get_unpublished_batch_rejects_invalid_batch_size() -> None:
    session = AsyncMock(spec=AsyncSession)
    repository = OutboxRepository(session)

    with pytest.raises(ValueError, match="batch_size must be at least 1"):
        await repository.get_unpublished_batch(0)

    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_unpublished_batch_executes_locked_query() -> None:
    session = AsyncMock(spec=AsyncSession)
    repository = OutboxRepository(session)

    first_event = OutboxEvent(
        id=uuid4(),
        event_type="payment.completed",
        payload={"status": "completed"},
        published=False,
    )
    second_event = OutboxEvent(
        id=uuid4(),
        event_type="payment.failed",
        payload={"status": "failed"},
        published=False,
    )

    scalar_result = MagicMock()
    scalar_result.all.return_value = [first_event, second_event]

    execute_result = MagicMock()
    execute_result.scalars.return_value = scalar_result

    session.execute.return_value = execute_result

    events = await repository.get_unpublished_batch(batch_size=2)

    assert events == [first_event, second_event]
    session.execute.assert_awaited_once()

    statement = session.execute.await_args.args[0]

    compiled_sql = str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    normalized_sql = " ".join(compiled_sql.split()).upper()

    assert "WHERE OUTBOX_EVENTS.PUBLISHED IS FALSE" in normalized_sql
    assert "ORDER BY OUTBOX_EVENTS.CREATED_AT, OUTBOX_EVENTS.ID" in normalized_sql
    assert "LIMIT 2" in normalized_sql
    assert "FOR UPDATE SKIP LOCKED" in normalized_sql


def test_mark_published_updates_event_flag() -> None:
    session = AsyncMock(spec=AsyncSession)
    repository = OutboxRepository(session)

    event = OutboxEvent(
        id=uuid4(),
        event_type="payment.completed",
        payload={"status": "completed"},
        published=False,
    )

    repository.mark_published(event)

    assert event.published is True
