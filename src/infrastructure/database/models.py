from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import UUID as UUIDType
from sqlalchemy import DateTime, ForeignKey, Index, String, func
from sqlalchemy import Enum as EnumType
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.infrastructure.database.config import Base


class LeadStatus(str, Enum):
    NEW = "new"
    APPROVED = "approved"
    REJECTED = "rejected"


class Lead(Base):
    """Represents a lead in the database."""

    __tablename__ = "leads"

    id: Mapped[UUID] = mapped_column(UUIDType, primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String, nullable=False)
    phone: Mapped[str] = mapped_column(String, nullable=False)
    source: Mapped[str] = mapped_column(String, nullable=False)
    comment: Mapped[str | None] = mapped_column(String, nullable=True)
    status: Mapped[LeadStatus] = mapped_column(
        EnumType(LeadStatus),
        nullable=False,
        default=LeadStatus.NEW,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class OutboxEvent(Base):
    """Represents an outbox event in the database."""

    __tablename__ = "outbox"
    __table_args__ = (Index("idx_outbox_unpublished", "published_at", "occurred_at"),)

    event_id: Mapped[UUID] = mapped_column(UUIDType, primary_key=True, default=uuid4)
    event_type: Mapped[str] = mapped_column(String, nullable=False)
    aggregate_id: Mapped[UUID] = mapped_column(
        UUIDType,
        ForeignKey("leads.id"),
        nullable=False,
    )
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )


class InboundEvent(Base):
    """Represents an inbound event in the database."""

    __tablename__ = "inbound_events"

    event_id: Mapped[UUID] = mapped_column(UUIDType, primary_key=True)
    event_type: Mapped[str] = mapped_column(String, nullable=False)
    aggregate_id: Mapped[UUID] = mapped_column(
        UUIDType,
        ForeignKey("leads.id"),
        nullable=False,
    )
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
