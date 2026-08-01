from app.models.outbox_event import OutboxEvent
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_unpublished_batch(self, batch_size: int) -> list[OutboxEvent]:
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        stmt = (
            select(OutboxEvent)
            .where(OutboxEvent.published.is_(False))
            .order_by(OutboxEvent.created_at, OutboxEvent.id)
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    def mark_published(self, event: OutboxEvent) -> None:
        event.published = True
