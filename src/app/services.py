import json
from datetime import UTC, datetime

from loguru import logger

from src.app.commands import CreateLeadCommand, GetLeadQuery, ProcessModerationCommand
from src.app.exceptions import LeadNotFoundError
from src.app.interfaces import MessageProducer, UnitOfWork
from src.domain.events import InboundEvent, OutboxEvent
from src.domain.lead import Lead


class CreateLeadService:
    """Service for creating a lead and publishing an outbox event."""

    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def execute(self, command: CreateLeadCommand) -> Lead:
        """Create a lead and persist it with an outbox event in one transaction."""
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
            logger.info("Lead created successfully")
        except Exception as e:
            await self._uow.rollback()
            logger.exception(f"Failed to create lead: {e}")
            raise

        return lead


class GetLeadService:
    """Service for retrieving a lead by ID."""

    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def execute(self, query: GetLeadQuery) -> Lead:
        """Get a lead by ID or raise LeadNotFoundError."""
        lead = await self._uow.leads.get_by_id(query.lead_id)
        if lead is None:
            logger.error(f"Lead not found: {query.lead_id}")
            raise LeadNotFoundError(query.lead_id)
        logger.info("Lead retrieved")
        return lead


class OutboxPublisherService:
    """Service for publishing unpublished outbox events to Kafka."""

    def __init__(self, uow: UnitOfWork, producer: MessageProducer) -> None:
        self._uow = uow
        self._producer = producer

    async def execute(self, topic: str) -> int:
        """Publish all unpublished outbox events to the given Kafka topic."""
        outbox_events = await self._uow.outbox.get_unpublished()
        if not outbox_events:
            logger.debug("No outbox events to publish")
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
                logger.info(f"Published event: {event.event_id} to topic: {topic}")
            await self._uow.commit()
        except Exception as e:
            await self._uow.rollback()
            logger.exception(f"Failed to publish event: {e}")
            raise
        logger.info(f"Published {len(outbox_events)} events to topic: {topic}")
        return len(outbox_events)


class ModerationConsumerService:
    """Service for processing moderation events from Kafka."""

    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def process(self, command: ProcessModerationCommand) -> bool:
        """Process a moderation event with idempotency check."""
        exists = await self._uow.inbound.exists(command.event_id)
        if exists:
            logger.debug(f"Skipped duplicate event: {command.event_id}")
            return False

        lead = await self._uow.leads.get_by_id(command.aggregate_id)
        if not lead:
            logger.warning(
                f"Lead not found: {command.aggregate_id} for event: {command.event_id}"
            )
            return False

        approved = command.payload.get("approved", False)
        lead.apply_moderation(approved)

        inbound = InboundEvent(
            event_id=command.event_id,
            event_type=command.event_type,
            aggregate_id=command.aggregate_id,
            payload=command.payload,
            received_at=datetime.now(tz=UTC),
        )

        try:
            await self._uow.inbound.add(inbound)
            await self._uow.leads.update_status(lead.id, lead.status)
            await self._uow.commit()
            logger.info(
                f"Processed event: {command.event_id} for lead: {command.aggregate_id}"
            )
        except Exception:
            await self._uow.rollback()
            raise

        return True
