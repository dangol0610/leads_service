from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from uuid import UUID, uuid4


class LeadStatus(str, Enum):
    NEW = "new"
    APPROVED = "approved"
    REJECTED = "rejected"


@dataclass(slots=True)
class Lead:
    id: UUID
    name: str
    phone: str
    source: str
    comment: str | None
    status: LeadStatus
    created_at: datetime
    updated_at: datetime

    @classmethod
    def create(
        cls,
        name: str,
        phone: str,
        source: str,
        comment: str | None,
    ) -> "Lead":
        now = datetime.now(UTC)
        return cls(
            id=uuid4(),
            name=name,
            phone=phone,
            source=source,
            comment=comment,
            status=LeadStatus.NEW,
            created_at=now,
            updated_at=now,
        )

    def apply_moderation(self, approved: bool) -> None:
        self.status = LeadStatus.APPROVED if approved else LeadStatus.REJECTED
        self.updated_at = datetime.now(tz=UTC)
