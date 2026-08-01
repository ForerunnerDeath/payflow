from collections.abc import Awaitable, Callable
from time import perf_counter
from uuid import uuid4

import structlog
from fastapi import Request, Response
from structlog.contextvars import bind_contextvars, clear_contextvars

logger = structlog.get_logger()


async def request_id_middleware(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    # Очищаем контекстные переменные перед обработкой запроса
    clear_contextvars()
    start_time = perf_counter()
    # Генерируем уникальный ID для каждого запроса
    request_id = str(uuid4())

    # Привязываем общие данные запроса к контексту логирования
    bind_contextvars(
        request_id=request_id, method=request.method, path=request.url.path
    )

    # Логируем начало обработки запроса
    logger.info("request_started")

    try:
        # Вызываем следующий middleware или обработчик запроса
        response = await call_next(request)
    except Exception:
        duration_ms = round((perf_counter() - start_time) * 1000, 2)
        logger.exception("request_failed", duration_ms=duration_ms)
        raise
    else:
        duration_ms = round((perf_counter() - start_time) * 1000, 2)
        # Добавляем request_id в заголовки ответа и логируем завершение обработки запроса
        response.headers["X-Request-ID"] = request_id
        logger.info(
            "request_finished",
            status_code=response.status_code,
            duration_ms=duration_ms,
        )
        return response
    finally:
        # Очищаем контекстные переменные после завершения обработки запроса
        clear_contextvars()
