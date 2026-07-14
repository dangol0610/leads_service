from dataclasses import dataclass
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
