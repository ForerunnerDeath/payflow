import asyncio
import random

import httpx

from app.schemas.provider import ProviderPaymentRequest, ProviderPaymentResponse


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
            try:
                response = await self._client.post(
                    "/process-payment", json=data.model_dump(mode="json")
                )
                response.raise_for_status()
                return ProviderPaymentResponse.model_validate(response.json())
            except httpx.HTTPStatusError as exc:
                status_code = exc.response.status_code
                if not 500 <= status_code < 600:
                    raise
                if attempt == self._max_attempts:
                    raise
            except httpx.RequestError:
                if attempt == self._max_attempts:
                    raise
            delay = self._retry_base_delay_seconds * 2 ** (
                attempt - 1
            ) + random.uniform(0, self._retry_base_delay_seconds)
            await asyncio.sleep(delay)
        raise RuntimeError("Payment provider retry loop finished unexpectedly")
