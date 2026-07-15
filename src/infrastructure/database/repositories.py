from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.domain.events import InboundEvent, OutboxEvent
from src.domain.lead import Lead, LeadStatus
from src.infrastructure.database.models import (
    InboundEventModel,
    LeadModel,
    OutboxEventModel,
)


class SqlAlchemyLeadRepository:
    """SQLAlchemy implementation of LeadRepository."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, lead: Lead) -> None:
        """Insert a new lead."""
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
        """Get a lead by ID, returning a domain Lead or None."""
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

    async def update_status(self, lead_id: UUID, status: LeadStatus) -> None:
        """Update lead status."""
        stmt = (
            update(LeadModel)
            .where(LeadModel.id == lead_id)
            .values(status=status, updated_at=func.now())
        )
        await self._session.execute(stmt)


class SqlAlchemyOutboxRepository:
    """SQLAlchemy implementation of OutboxRepository."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, event: OutboxEvent) -> None:
        """Insert an outbox event."""
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
        """Get unpublished outbox events with row-level locking."""
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
        """Mark an outbox event as published."""
        stmt = (
            update(OutboxEventModel)
            .where(OutboxEventModel.event_id == event_id)
            .values(published_at=datetime.now(tz=UTC))
        )
        await self._session.execute(stmt)


class SqlAlchemyInboundEventRepository:
    """SQLAlchemy implementation of InboundEventRepository."""

    def __init__(self, session: AsyncSession):
        self._session = session

    async def add(self, event: InboundEvent) -> None:
        """Insert an inbound event for idempotency tracking."""
        stmt = insert(InboundEventModel).values(
            event_id=event.event_id,
            event_type=event.event_type,
            aggregate_id=event.aggregate_id,
            payload=event.payload,
            received_at=event.received_at,
        )
        await self._session.execute(stmt)

    async def exists(self, event_id: UUID) -> bool:
        """Check if an inbound event already exists."""
        stmt = select(InboundEventModel).where(InboundEventModel.event_id == event_id)
        result = await self._session.execute(stmt)
        if result.scalar():
            return True
        return False
