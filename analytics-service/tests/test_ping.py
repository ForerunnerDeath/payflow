from uuid import UUID

import pytest
from app.main import app
from httpx import ASGITransport, AsyncClient


@pytest.mark.asyncio
async def test_ping_returns_ok_with_request_id() -> None:
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/ping")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

    request_id = response.headers["X-Request-ID"]
    assert str(UUID(request_id)) == request_id
