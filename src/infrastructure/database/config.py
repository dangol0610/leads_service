from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase


class Database:
    """Async database engine and session factory.

    Manages the connection pool and provides sessions for the application.
    """

    def __init__(
        self,
        database_url: str,
        pool_size: int,
        max_overflow: int,
        pool_recycle: int,
        pool_pre_ping: bool,
        echo: bool = False,
    ) -> None:
        self.engine: AsyncEngine = create_async_engine(
            url=database_url,
            echo=echo,
            pool_pre_ping=pool_pre_ping,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_recycle=pool_recycle,
        )
        self.session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
            bind=self.engine,
            expire_on_commit=False,
        )

    async def close(self) -> None:
        """Dispose the database engine and release all connections."""
        await self.engine.dispose()

    async def get_session(self) -> AsyncGenerator[AsyncSession]:
        """Yield an async session that auto-closes on exit."""
        async with self.session_factory() as session:
            yield session


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models."""

    pass
