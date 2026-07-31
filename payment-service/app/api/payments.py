from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import (
    get_payment_provider_circuit_breaker,
    get_payment_provider_client,
)
from app.clients.circuit_breaker import CircuitBreaker
from app.clients.payment_provider import PaymentProviderClient
from app.core.database import get_session
from app.schemas.payment import PaymentCreate, PaymentResponse
from app.services.payment import PaymentService

router = APIRouter(prefix="/payments", tags=["payments"])


@router.post("", response_model=PaymentResponse, status_code=status.HTTP_201_CREATED)
async def create_payment(
    data: PaymentCreate,
    session: Annotated[AsyncSession, Depends(get_session)],
    provider_client: Annotated[
        PaymentProviderClient, Depends(get_payment_provider_client)
    ],
    circuit_breaker: Annotated[
        CircuitBreaker, Depends(get_payment_provider_circuit_breaker)
    ],
) -> PaymentResponse:
    service = PaymentService(
        session=session,
        provider_client=provider_client,
        circuit_breaker=circuit_breaker,
    )
    payment = await service.create_payment(data)
    return PaymentResponse.model_validate(payment)


@router.get("/{payment_id}", response_model=PaymentResponse)
async def get_payment(
    payment_id: UUID,
    session: Annotated[AsyncSession, Depends(get_session)],
    provider_client: Annotated[
        PaymentProviderClient, Depends(get_payment_provider_client)
    ],
    circuit_breaker: Annotated[
        CircuitBreaker, Depends(get_payment_provider_circuit_breaker)
    ],
) -> PaymentResponse:
    service = PaymentService(
        session=session,
        provider_client=provider_client,
        circuit_breaker=circuit_breaker,
    )
    payment = await service.get_payment(payment_id)
    if payment is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Платеж с ID {payment_id} не найден",
        )
    return PaymentResponse.model_validate(payment)
