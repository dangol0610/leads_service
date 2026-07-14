from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.domain.events import OutboxEvent
from src.domain.lead import Lead
from src.infrastructure.database.models import LeadModel, OutboxEventModel


class SqlAlchemyLeadRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, lead: Lead) -> None:
        stmt = insert(LeadModel).values(
            id=lead.id,
            name=lead.name,
            phone=lead.phone,
            source=lead.source,
            comment=lead.comment,
            status=lead.status,
            created_at=lead.created_at,
            updated_at=lead.updated_at,
        )
        await self._session.execute(stmt)


class SqlAlchemyOutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, event: OutboxEvent) -> None:
        stmt = insert(OutboxEventModel).values(
            event_id=event.event_id,
            event_type=event.event_type,
            aggregate_id=event.aggregate_id,
            payload=event.payload,
            occurred_at=event.occurred_at,
            published_at=event.published_at,
        )
        await self._session.execute(stmt)
