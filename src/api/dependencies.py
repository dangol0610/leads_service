from typing import AsyncGenerator

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from src.app.interfaces import UnitOfWork
from src.app.services import CreateLeadService
from src.infrastructure.database.config import Database
from src.infrastructure.database.uow import SqlAlchemyUnitOfWork


async def get_session(request: Request) -> AsyncGenerator[AsyncSession, None]:
    database: Database = request.app.state.database
    async for session in database.get_session():
        yield session


async def get_create_lead_service(
    session: AsyncSession = Depends(get_session),
) -> CreateLeadService:
    uow: UnitOfWork = SqlAlchemyUnitOfWork(session)
    return CreateLeadService(uow)
