from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from pydantic_extra_types.phone_numbers import PhoneNumber

from src.domain.lead import Lead, LeadStatus


class CreateLeadRequest(BaseModel):
    """Request schema for creating a lead."""

    name: str = Field(min_length=1, max_length=255)
    phone: PhoneNumber
    source: str = Field(min_length=1, max_length=255)
    comment: str = Field(min_length=1, max_length=255)

    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")


class LeadResponse(BaseModel):
    """Response schema for a lead."""

    id: UUID
    name: str
    phone: str
    source: str
    comment: str | None
    status: LeadStatus
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_domain(cls, lead: Lead) -> "LeadResponse":
        """Convert a domain Lead to a response schema."""
        return cls(
            id=lead.id,
            name=lead.name,
            phone=lead.phone,
            source=lead.source,
            comment=lead.comment,
            status=lead.status,
            created_at=lead.created_at,
            updated_at=lead.updated_at,
        )
