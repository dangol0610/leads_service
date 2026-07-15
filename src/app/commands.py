from dataclasses import dataclass
from typing import Any
from uuid import UUID


@dataclass(frozen=True, slots=True)
class CreateLeadCommand:
    """Command to create a new lead."""

    name: str
    phone: str
    source: str
    comment: str


@dataclass(frozen=True, slots=True)
class GetLeadQuery:
    """Query to get a lead by ID."""

    lead_id: UUID


@dataclass(frozen=True, slots=True)
class ProcessModerationCommand:
    """Command to process a moderation event from Kafka."""

    event_id: UUID
    event_type: str
    aggregate_id: UUID
    payload: dict[str, Any]
