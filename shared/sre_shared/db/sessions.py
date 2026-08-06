"""
Async SQLAlchemy engine and session factory.

Usage::

    from sre_shared.db.session import get_session

    async with get_session() as session:
        result = await session.execute(select(Incident))
        incidents = result.scalars().all()

Design notes
------------
- get_session()      -> auto-commits on clean exit, rolls back on exception.
                         Use this for normal write-path agent code.
- get_db_dependency()-> does NOT auto-commit. Intended for FastAPI route
                         handlers (dashboard/api) that want explicit control
                         over transaction boundaries within a single request.

"""


from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import ( 
    AsyncSession, 
    AsyncEngine, 
    async_sessionmaker, 
    create_async_engine, 
)

from sre_shared.config.settings import settings


_engine: AsyncEngine = create_async_engine(
    settings.postgres_url,
    echo=settings.environment == "development",  # TODO: Set to False in production to disable SQL Logging
    future=True,
    pool_size=settings.postgres_pool_size,
    max_overflow=settings.postgres_max_overflow,
    pool_pre_ping=True,
)

_session_factory = async_sessionmaker(
    bind=_engine,
    autoflush=False,
    class_=AsyncSession,
    autocommit=False,
    expire_on_commit=False,
)

@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Async context manager that yields a database session.

    Automatically commits on clean exit and rolls back on exception.

    Example::

        async with get_session() as session:
            session.add(incident)
            # commit happens automatically on exit
    """

    async with _session_factory() as session:
        try:
            yield session
            await session.commit()
        except BaseException:
            await session.rollback()
            raise
        finally:
            await session.close()

async def get_db_dependency() -> AsyncGenerator[AsyncSession, None]:
    async with _session_factory() as session:
        try:
            yield session
        finally:
            await session.close()

async def dispose_engine() -> None:
    """
    Dispose of the engine connection pool.

    Call during application shutdown to cleanly close all DB connections.
    """
    await _engine.dispose()

