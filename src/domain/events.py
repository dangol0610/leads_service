from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from src.domain.lead import Lead


@dataclass(slots=True)
class OutboxEvent:
    """An event to be published to Kafka via the outbox pattern."""

    event_id: UUID
    event_type: str
    aggregate_id: UUID
    payload: dict[str, Any]
    occurred_at: datetime
    published_at: datetime | None = None

    @classmethod
    def lead_created(cls, lead: Lead) -> "OutboxEvent":
        """Create a lead_created.v1 outbox event from a lead."""
        return cls(
            event_id=uuid4(),
            event_type="lead_created.v1",
            aggregate_id=lead.id,
            occurred_at=lead.created_at,
            payload={
                "lead_id": str(lead.id),
                "name": lead.name,
                "phone": lead.phone,
                "source": lead.source,
            },
        )

    def mark_published(self) -> None:
        """Mark the event as published."""
        self.published_at = datetime.now(tz=UTC)


@dataclass(slots=True)
class InboundEvent:
    """An event received from Kafka after processing."""

    event_id: UUID
    event_type: str
    aggregate_id: UUID
    payload: dict[str, Any]
    received_at: datetime | None = None
