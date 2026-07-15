import json

from src.app.commands import CreateLeadCommand, GetLeadQuery
from src.app.exceptions import LeadNotFoundError
from src.app.interfaces import MessageProducer, UnitOfWork
from src.domain.events import OutboxEvent
from src.domain.lead import Lead


class CreateLeadService:
    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def execute(self, command: CreateLeadCommand) -> Lead:
        lead = Lead.create(
            name=command.name,
            phone=command.phone,
            source=command.source,
            comment=command.comment,
        )
        event = OutboxEvent.lead_created(lead)

        try:
            await self._uow.leads.add(lead)
            await self._uow.outbox.add(event)
            await self._uow.commit()
        except Exception:
            await self._uow.rollback()
            raise

        return lead


class GetLeadService:
    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def execute(self, query: GetLeadQuery) -> Lead:
        lead = await self._uow.leads.get_by_id(query.lead_id)

        if lead is None:
            raise LeadNotFoundError(query.lead_id)

        return lead


class OutboxPublisherService:
    def __init__(self, uow: UnitOfWork, producer: MessageProducer) -> None:
        self._uow = uow
        self._producer = producer

    async def execute(self, topic: str) -> int:
        outbox_events = await self._uow.outbox.get_unpublished()
        if not outbox_events:
            return 0
        try:
            for event in outbox_events:
                payload = json.dumps(
                    {
                        "event_id": str(event.event_id),
                        "event_type": event.event_type,
                        "aggregate_id": str(event.aggregate_id),
                        "occurred_at": event.occurred_at.isoformat(),
                        "payload": event.payload,
                    }
                ).encode()
                await self._producer.send(
                    topic=topic,
                    key=str(event.aggregate_id),
                    value=payload,
                )
                await self._uow.outbox.mark_as_published(event.event_id)
            await self._uow.commit()
        except Exception:
            await self._uow.rollback()
            raise

        return len(outbox_events)
