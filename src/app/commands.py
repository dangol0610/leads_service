from dataclasses import dataclass
from typing import Any
from uuid import UUID


@dataclass(frozen=True, slots=True)
class CreateLeadCommand:
    name: str
    phone: str
    source: str
    comment: str


@dataclass(frozen=True, slots=True)
class GetLeadQuery:
    lead_id: UUID


@dataclass(frozen=True, slots=True)
class ProcessModerationCommand:
    event_id: UUID
    event_type: str
    aggregate_id: UUID
    payload: dict[str, Any]
