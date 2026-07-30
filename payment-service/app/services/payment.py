from uuid import UUID

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.payment import Payment
from app.repositories.payment import PaymentRepository
from app.schemas.payment import PaymentCreate


class PaymentService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._repository = PaymentRepository(session)

    async def create_payment(self, data: PaymentCreate) -> Payment:
        existing_payment = await self._repository.get_by_idempotency_key(
            data.idempotency_key
        )
        if existing_payment is not None:
            return existing_payment
        payment = Payment(
            amount=data.amount,
            currency=data.currency,
            description=data.description,
            idempotency_key=data.idempotency_key,
            customer_id=data.customer_id,
        )
        try:
            payment = await self._repository.add(payment)
            await self._session.commit()
        except IntegrityError:
            await self._session.rollback()
            existing_payment = await self._repository.get_by_idempotency_key(
                data.idempotency_key
            )
            if existing_payment is None:
                raise
            return existing_payment
        except Exception:
            await self._session.rollback()
            raise
        await self._session.refresh(payment)
        return payment

    async def get_payment(self, payment_id: UUID) -> Payment | None:
        return await self._repository.get_by_id(payment_id)
