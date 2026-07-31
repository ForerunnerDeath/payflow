import asyncio
import random
from uuid import uuid4

from fastapi import FastAPI, HTTPException, status

from app.schemas import ProcessPaymentRequest, ProcessPaymentResponse

app = FastAPI(
    title="PayFlow Mock Provider",
    version="0.1.0",
)


@app.get("/ping")
async def ping() -> dict[str, str]:
    return {
        "status": "ok",
    }


@app.post("/process-payment", response_model=ProcessPaymentResponse)
async def process_payment(payment: ProcessPaymentRequest) -> ProcessPaymentResponse:
    roll = random.random()
    if roll < 0.05:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Mock provider internal error",
        )
    elif roll < 0.10:
        await asyncio.sleep(10)
    return ProcessPaymentResponse(provider_payment_id=uuid4(), status="approved")
