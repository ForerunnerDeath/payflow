from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.processed_event import ProcessedEvent
from app.models.transaction import Transaction
from app.schemas.event import PaymentEvent


class PaymentEventProcessor:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def process(self, event: PaymentEvent) -> bool:
        async with self._session_factory() as session, session.begin():
            processed_event_statement = (
                insert(ProcessedEvent)
                .values(event_id=event.event_id)
                .on_conflict_do_nothing(
                    index_elements=[ProcessedEvent.event_id],
                )
                .returning(ProcessedEvent.event_id)
            )
            result = await session.execute(processed_event_statement)
            inserted_event_id = result.scalar_one_or_none()
            if inserted_event_id is None:
                return False

            transaction_statement = (
                insert(Transaction)
                .values(
                    payment_id=event.payment_id,
                    amount=event.amount,
                    currency=event.currency,
                    status=event.status,
                    event_type=event.event_type,
                )
                .on_conflict_do_update(
                    index_elements=[Transaction.payment_id],
                    set_={
                        "amount": event.amount,
                        "currency": event.currency,
                        "status": event.status,
                        "event_type": event.event_type,
                        "processed_at": func.now(),
                    },
                )
            )

            await session.execute(transaction_statement)

        return True
