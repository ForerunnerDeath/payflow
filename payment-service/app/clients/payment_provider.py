import asyncio
import random

import httpx
import structlog

from app.schemas.provider import ProviderPaymentRequest, ProviderPaymentResponse

logger = structlog.get_logger()

IDEMPOTENCY_HEADER = "Idempotency-Key"


class PaymentProviderClient:
    def __init__(
        self,
        client: httpx.AsyncClient,
        max_attempts: int,
        retry_base_delay_seconds: float,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")

        if retry_base_delay_seconds < 0:
            raise ValueError("retry_base_delay_seconds must not be negative")

        self._client = client
        self._max_attempts = max_attempts
        self._retry_base_delay_seconds = retry_base_delay_seconds

    async def process_payment(
        self, data: ProviderPaymentRequest
    ) -> ProviderPaymentResponse:
        for attempt in range(1, self._max_attempts + 1):
            retry_error_type: str | None = None
            retry_status_code: int | None = None
            try:
                response = await self._client.post(
                    "/process-payment",
                    json=data.model_dump(mode="json"),
                    headers={IDEMPOTENCY_HEADER: str(data.payment_id)},
                )
                response.raise_for_status()
                return ProviderPaymentResponse.model_validate(response.json())
            except httpx.HTTPStatusError as exc:
                status_code = exc.response.status_code
                if not 500 <= status_code < 600:
                    raise
                if attempt == self._max_attempts:
                    logger.error(
                        "payment_provider_retry_exhausted",
                        attempt=attempt,
                        max_attempts=self._max_attempts,
                        error_type=type(exc).__name__,
                        status_code=status_code,
                    )
                    raise
                retry_error_type = type(exc).__name__
                retry_status_code = status_code
            except httpx.RequestError as exc:
                if attempt == self._max_attempts:
                    logger.error(
                        "payment_provider_retry_exhausted",
                        attempt=attempt,
                        max_attempts=self._max_attempts,
                        error_type=type(exc).__name__,
                        status_code=None,
                    )
                    raise
                retry_error_type = type(exc).__name__
                retry_status_code = None
            delay = self._retry_base_delay_seconds * 2 ** (
                attempt - 1
            ) + random.uniform(0, self._retry_base_delay_seconds)

            logger.warning(
                "payment_provider_retry_scheduled",
                attempt=attempt,
                next_attempt=attempt + 1,
                max_attempts=self._max_attempts,
                delay_seconds=round(delay, 3),
                error_type=retry_error_type,
                status_code=retry_status_code,
            )

            await asyncio.sleep(delay)
        raise RuntimeError("Payment provider retry loop finished unexpectedly")
