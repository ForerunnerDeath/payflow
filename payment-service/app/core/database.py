from collections.abc import AsyncIterator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import Settings

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def init_db(settings: Settings) -> None:
    global _engine, _session_factory
    # Инициализация асинхронного движка SQLAlchemy и фабрики сессий
    if _engine is not None:
        raise RuntimeError("Database engine already initialized.")
    _engine = create_async_engine(settings.database_url, pool_pre_ping=True)
    _session_factory = async_sessionmaker(bind=_engine, expire_on_commit=False)


async def get_session() -> AsyncIterator[AsyncSession]:
    # Создает и возвращает асинхронную сессию SQLAlchemy
    if _session_factory is None:
        raise RuntimeError("SessionFactory is not initialized. Call init_db first.")
    # Используем контекстный менеджер для управления жизненным циклом сессии
    async with _session_factory() as session:
        yield session


async def close_db() -> None:
    # Закрывает соединение с базой данных и очищает ресурсы
    global _engine, _session_factory

    engine = _engine
    _engine = None
    _session_factory = None

    if engine is not None:
        await engine.dispose()


async def check_db_connection() -> None:
    # Проверяет соединение с базой данных, выполняя простой запрос
    if _engine is None:
        raise RuntimeError("Database engine is not initialized. Call init_db first.")
    async with _engine.connect() as connection:
        await connection.execute(text("SELECT 1"))


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    if _session_factory is None:
        raise RuntimeError("SessionFactory is not initialized. Call init_db first.")
    return _session_factory
