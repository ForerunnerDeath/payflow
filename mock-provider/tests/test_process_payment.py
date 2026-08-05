import asyncio
import importlib
from types import ModuleType
from unittest.mock import Mock
from uuid import uuid4

import app.main as main_module
import pytest
from httpx import ASGITransport, AsyncClient


@pytest.fixture
def provider_module() -> ModuleType:
    # Перезагрузка модуля даёт каждому тесту чистое in-memory состояние.
    return importlib.reload(main_module)


def make_payment_payload(
    *,
    payment_id: str,
    amount: str = "500.15",
    currency: str = "RUB",
) -> dict[str, str]:
    return {
        "payment_id": payment_id,
        "amount": amount,
        "currency": currency,
    }


@pytest.mark.asyncio
async def test_repeated_request_returns_same_provider_result(
    provider_module: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payment_id = uuid4()
    provider_payment_id = uuid4()

    uuid_mock = Mock(return_value=provider_payment_id)

    monkeypatch.setattr(provider_module.random, "random", lambda: 0.5)
    monkeypatch.setattr(provider_module, "uuid4", uuid_mock)

    transport = ASGITransport(app=provider_module.app)

    async with AsyncClient(
        transport=transport,
        base_url="http://provider.test",
    ) as client:
        payload = make_payment_payload(payment_id=str(payment_id))
        headers = {
            "Idempotency-Key": str(payment_id),
        }

        first_response = await client.post(
            "/process-payment",
            json=payload,
            headers=headers,
        )
        second_response = await client.post(
            "/process-payment",
            json=payload,
            headers=headers,
        )

    assert first_response.status_code == 200
    assert second_response.status_code == 200

    assert first_response.json() == second_response.json()
    assert first_response.json() == {
        "provider_payment_id": str(provider_payment_id),
        "status": "approved",
    }

    # Бизнес-операция должна быть создана только один раз.
    uuid_mock.assert_called_once_with()


@pytest.mark.asyncio
async def test_reused_key_with_different_payload_returns_conflict(
    provider_module: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payment_id = uuid4()

    monkeypatch.setattr(provider_module.random, "random", lambda: 0.5)

    transport = ASGITransport(app=provider_module.app)

    async with AsyncClient(
        transport=transport,
        base_url="http://provider.test",
    ) as client:
        headers = {
            "Idempotency-Key": str(payment_id),
        }

        first_response = await client.post(
            "/process-payment",
            json=make_payment_payload(
                payment_id=str(payment_id),
                amount="500.15",
            ),
            headers=headers,
        )
        conflicting_response = await client.post(
            "/process-payment",
            json=make_payment_payload(
                payment_id=str(payment_id),
                amount="700.00",
            ),
            headers=headers,
        )

    assert first_response.status_code == 200
    assert conflicting_response.status_code == 409
    assert conflicting_response.json() == {
        "detail": ("Idempotency key was already used with a different payment payload"),
    }


@pytest.mark.asyncio
async def test_missing_idempotency_key_is_rejected(
    provider_module: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(provider_module.random, "random", lambda: 0.5)

    transport = ASGITransport(app=provider_module.app)

    async with AsyncClient(
        transport=transport,
        base_url="http://provider.test",
    ) as client:
        response = await client.post(
            "/process-payment",
            json=make_payment_payload(payment_id=str(uuid4())),
        )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_retry_during_delayed_response_returns_stored_result(
    provider_module: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payment_id = uuid4()
    first_provider_payment_id = uuid4()
    second_provider_payment_id = uuid4()

    response_delay_started = asyncio.Event()
    release_delayed_response = asyncio.Event()

    async def delay_response(_: float) -> None:
        response_delay_started.set()
        await release_delayed_response.wait()

    # Первая попытка имитирует уже выполненную операцию с задержанным ответом.
    # Второе значение понадобилось бы только старой неидемпотентной реализации.
    roll_mock = Mock(side_effect=[0.07, 0.5])
    uuid_mock = Mock(
        side_effect=[
            first_provider_payment_id,
            second_provider_payment_id,
        ]
    )

    monkeypatch.setattr(provider_module.random, "random", roll_mock)
    monkeypatch.setattr(provider_module.asyncio, "sleep", delay_response)
    monkeypatch.setattr(provider_module, "uuid4", uuid_mock)

    transport = ASGITransport(app=provider_module.app)

    async with AsyncClient(
        transport=transport,
        base_url="http://provider.test",
    ) as client:
        payload = make_payment_payload(payment_id=str(payment_id))
        headers = {
            "Idempotency-Key": str(payment_id),
        }

        first_request = asyncio.create_task(
            client.post(
                "/process-payment",
                json=payload,
                headers=headers,
            )
        )

        await asyncio.wait_for(
            response_delay_started.wait(),
            timeout=1.0,
        )

        retry_response = await asyncio.wait_for(
            client.post(
                "/process-payment",
                json=payload,
                headers=headers,
            ),
            timeout=1.0,
        )

        release_delayed_response.set()
        first_response = await first_request

    assert first_response.status_code == 200
    assert retry_response.status_code == 200
    assert first_response.json() == retry_response.json()

    assert first_response.json() == {
        "provider_payment_id": str(first_provider_payment_id),
        "status": "approved",
    }

    # Повтор не должен снова выполнять операцию, вызывать random или
    # генерировать новый provider_payment_id.
    assert roll_mock.call_count == 1
    assert uuid_mock.call_count == 1
