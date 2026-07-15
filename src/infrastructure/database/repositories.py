from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import insert, select, update
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

    async def get_by_id(self, lead_id: UUID) -> Lead | None:
        query = select(LeadModel).where(LeadModel.id == lead_id)
        result = await self._session.execute(query)
        orm_lead = result.scalar_one_or_none()

        if orm_lead is None:
            return None
        return Lead(
            id=orm_lead.id,
            name=orm_lead.name,
            phone=orm_lead.phone,
            source=orm_lead.source,
            comment=orm_lead.comment,
            status=orm_lead.status,
            created_at=orm_lead.created_at,
            updated_at=orm_lead.updated_at,
        )


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

    async def get_unpublished(self) -> list[OutboxEvent]:
        query = (
            select(OutboxEventModel)
            .where(OutboxEventModel.published_at.is_(None))
            .order_by(OutboxEventModel.occurred_at)
            .limit(100)
            .with_for_update(skip_locked=True)
        )
        result = await self._session.execute(query)
        orm_events = result.scalars().all()
        return [
            OutboxEvent(
                event_id=orm_event.event_id,
                event_type=orm_event.event_type,
                aggregate_id=orm_event.aggregate_id,
                payload=orm_event.payload,
                occurred_at=orm_event.occurred_at,
                published_at=orm_event.published_at,
            )
            for orm_event in orm_events
        ]

    async def mark_as_published(self, event_id: UUID) -> None:
        stmt = (
            update(OutboxEventModel)
            .where(OutboxEventModel.event_id == event_id)
            .values(published_at=datetime.now(tz=UTC))
        )
        await self._session.execute(stmt)
