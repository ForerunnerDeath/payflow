from typing import cast

import app.core.middleware as middleware_module
import pytest
from fastapi import Request, Response
from starlette.types import Scope


class RecordingLogger:
    def __init__(self) -> None:
        self.info_events: list[tuple[str, dict[str, object]]] = []
        self.exception_events: list[tuple[str, dict[str, object]]] = []

    def info(self, event: str, **kwargs: object) -> None:
        self.info_events.append((event, kwargs))

    def exception(self, event: str, **kwargs: object) -> None:
        self.exception_events.append((event, kwargs))


@pytest.mark.asyncio
async def test_request_middleware_logs_successful_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger = RecordingLogger()
    bound_context: dict[str, object] = {}

    timer_values = iter([100.0, 100.125])

    def fake_bind_contextvars(**kwargs: object) -> None:
        bound_context.update(kwargs)

    monkeypatch.setattr(middleware_module, "logger", logger)
    monkeypatch.setattr(middleware_module, "bind_contextvars", fake_bind_contextvars)
    monkeypatch.setattr(middleware_module, "perf_counter", lambda: next(timer_values))

    scope = cast(
        Scope,
        {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": "/test",
            "raw_path": b"/test",
            "query_string": b"",
            "headers": [],
            "client": ("test-client", 50000),
            "server": ("test-server", 80),
            "root_path": "",
        },
    )
    request = Request(scope)

    async def call_next(_: Request) -> Response:
        return Response(status_code=204)

    response = await middleware_module.request_id_middleware(request, call_next)

    request_id = bound_context["request_id"]

    assert isinstance(request_id, str)
    assert bound_context["method"] == "GET"
    assert bound_context["path"] == "/test"

    assert response.status_code == 204
    assert response.headers["X-Request-ID"] == request_id

    assert logger.info_events[0] == ("request_started", {})

    finished_event, finished_fields = logger.info_events[1]

    assert finished_event == "request_finished"
    assert finished_fields == {
        "status_code": 204,
        "duration_ms": 125.0,
    }


@pytest.mark.asyncio
async def test_request_middleware_logs_failed_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger = RecordingLogger()
    timer_values = iter([200.0, 200.25])

    monkeypatch.setattr(middleware_module, "logger", logger)
    monkeypatch.setattr(middleware_module, "perf_counter", lambda: next(timer_values))

    scope = cast(
        Scope,
        {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/test-error",
            "raw_path": b"/test-error",
            "query_string": b"",
            "headers": [],
            "client": ("test-client", 50000),
            "server": ("test-server", 80),
            "root_path": "",
        },
    )
    request = Request(scope)

    async def call_next(_: Request) -> Response:
        raise ValueError("endpoint failed")

    with pytest.raises(ValueError, match="endpoint failed"):
        await middleware_module.request_id_middleware(request, call_next)

    assert logger.info_events == [("request_started", {})]
    assert logger.exception_events == [
        (
            "request_failed",
            {
                "duration_ms": 250.0,
            },
        ),
    ]
