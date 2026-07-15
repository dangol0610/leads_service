from sqlalchemy.ext.asyncio import AsyncSession

from src.infrastructure.database.repositories import (
    SqlAlchemyInboundEventRepository,
    SqlAlchemyLeadRepository,
    SqlAlchemyOutboxRepository,
)


class SqlAlchemyUnitOfWork:
    """SQLAlchemy implementation of UnitOfWork.

    Wraps a single database session and provides access to all repositories.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self.leads = SqlAlchemyLeadRepository(session)
        self.outbox = SqlAlchemyOutboxRepository(session)
        self.inbound = SqlAlchemyInboundEventRepository(session)

    async def commit(self) -> None:
        """Commit the current transaction."""
        await self._session.commit()

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        await self._session.rollback()
