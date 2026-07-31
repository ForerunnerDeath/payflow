from typing import cast

from fastapi import Request

from app.clients.circuit_breaker import CircuitBreaker
from app.clients.payment_provider import PaymentProviderClient


def get_payment_provider_client(request: Request) -> PaymentProviderClient:
    return cast(PaymentProviderClient, request.app.state.payment_provider_client)


def get_payment_provider_circuit_breaker(request: Request) -> CircuitBreaker:
    return cast(CircuitBreaker, request.app.state.payment_provider_circuit_breaker)
