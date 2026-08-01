from datetime import UTC, datetime
from uuid import UUID

import httpx
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.clients.circuit_breaker import CircuitBreaker, CircuitBreakerOpenError
from app.clients.payment_provider import PaymentProviderClient
from app.models.outbox_event import OutboxEvent
from app.models.payment import Payment, PaymentStatus
from app.repositories.payment import PaymentRepository
from app.schemas.event import PaymentEventPayload
from app.schemas.payment import PaymentCreate
from app.schemas.provider import ProviderPaymentRequest


class PaymentService:
    def __init__(
        self,
        session: AsyncSession,
        provider_client: PaymentProviderClient,
        circuit_breaker: CircuitBreaker,
    ) -> None:
        self._session = session
        self._repository = PaymentRepository(session)
        self._provider_client = provider_client
        self._circuit_breaker = circuit_breaker

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
        payment.status = PaymentStatus.PROCESSING
        await self._session.commit()
        provider_request = ProviderPaymentRequest(
            payment_id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
        )
        try:
            provider_response = await self._circuit_breaker.call(
                self._provider_client.process_payment,
                provider_request,
            )
        except (httpx.HTTPError, CircuitBreakerOpenError, ValidationError) as exc:
            payment.status = PaymentStatus.FAILED
            payment.failure_reason = str(exc)
            payment.provider_payment_id = None
            payment.completed_at = None
            outbox_event = self._build_outbox_event(payment)
            self._session.add(outbox_event)
            await self._session.commit()
            await self._session.refresh(payment)
            return payment
        payment.status = PaymentStatus.COMPLETED
        payment.provider_payment_id = str(provider_response.provider_payment_id)
        payment.completed_at = datetime.now(UTC)
        payment.failure_reason = None
        outbox_event = self._build_outbox_event(payment)
        self._session.add(outbox_event)
        await self._session.commit()
        await self._session.refresh(payment)
        return payment

    async def get_payment(self, payment_id: UUID) -> Payment | None:
        return await self._repository.get_by_id(payment_id)

    def _build_outbox_event(self, payment: Payment) -> OutboxEvent:
        if payment.status == PaymentStatus.COMPLETED:
            event_type = "payment.completed"
        elif payment.status == PaymentStatus.FAILED:
            event_type = "payment.failed"
        else:
            raise ValueError("payment.status is wrong")
        payload = PaymentEventPayload(
            event_type=event_type,
            payment_id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
            status=payment.status,
        )
        return OutboxEvent(
            id=payload.event_id,
            event_type=payload.event_type,
            payload=payload.model_dump(mode="json"),
        )
