from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from src.domain.events import InboundEvent, OutboxEvent
from src.domain.lead import Lead, LeadStatus


class LeadRepository(Protocol):
    """Repository interface for Lead persistence."""

    async def add(self, lead: Lead) -> None: ...
    async def get_by_id(self, lead_id: UUID) -> Lead | None: ...
    async def update_status(self, lead_id: UUID, status: LeadStatus) -> None: ...


class OutboxRepository(Protocol):
    """Repository interface for OutboxEvent persistence."""

    async def add(self, event: OutboxEvent) -> None: ...
    async def get_unpublished(self) -> list[OutboxEvent]: ...
    async def mark_as_published(self, event_id: UUID) -> None: ...


class InboundEventRepository(Protocol):
    """Repository interface for inbound event idempotency."""

    async def exists(self, event_id: UUID) -> bool: ...
    async def add(self, event: InboundEvent) -> None: ...


class UnitOfWork(Protocol):
    """Unit of Work interface for transactional consistency."""

    @property
    def leads(self) -> LeadRepository: ...

    @property
    def outbox(self) -> OutboxRepository: ...

    @property
    def inbound(self) -> InboundEventRepository: ...

    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...


class MessageProducer(Protocol):
    """Interface for producing Kafka messages."""

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def send(self, topic: str, key: str, value: bytes) -> None: ...


@dataclass(slots=True)
class ConsumerMessage:
    """Raw Kafka consumer message DTO."""

    topic: str
    key: bytes | None
    value: bytes | None
    offset: int
    partition: int


class MessageConsumer(Protocol):
    """Interface for consuming Kafka messages."""

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def poll(self, timeout_ms: int) -> list[ConsumerMessage]: ...
    async def commit(self) -> None: ...
