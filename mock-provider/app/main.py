import asyncio
import random
from dataclasses import dataclass
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import FastAPI, Header, HTTPException, status

from app.schemas import ProcessPaymentRequest, ProcessPaymentResponse


@dataclass(frozen=True, slots=True)
class IdempotencyRecord:
    request: ProcessPaymentRequest
    response: ProcessPaymentResponse


_idempotency_records: dict[UUID, IdempotencyRecord] = {}
_idempotency_locks: dict[UUID, asyncio.Lock] = {}


app = FastAPI(title="PayFlow Mock Provider", version="0.1.0")


@app.get("/ping")
async def ping() -> dict[str, str]:
    return {
        "status": "ok",
    }


@app.post("/process-payment", response_model=ProcessPaymentResponse)
async def process_payment(
    payment: ProcessPaymentRequest,
    idempotency_key: Annotated[
        UUID,
        Header(alias="Idempotency-Key"),
    ],
) -> ProcessPaymentResponse:
    lock = _idempotency_locks.setdefault(
        idempotency_key,
        asyncio.Lock(),
    )

    should_delay_response = False

    async with lock:
        existing_record = _idempotency_records.get(idempotency_key)

        if existing_record is not None:
            if existing_record.request != payment:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=(
                        "Idempotency key was already used "
                        "with a different payment payload"
                    ),
                )

            return existing_record.response

        roll = random.random()

        if roll < 0.05:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Mock provider internal error",
            )

        response = ProcessPaymentResponse(
            provider_payment_id=uuid4(),
            status="approved",
        )

        _idempotency_records[idempotency_key] = IdempotencyRecord(
            request=payment,
            response=response,
        )

        should_delay_response = roll < 0.10

    if should_delay_response:
        await asyncio.sleep(10)

    return response
