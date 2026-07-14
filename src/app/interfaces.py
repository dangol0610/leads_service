from typing import Protocol
from uuid import UUID

from src.domain.events import OutboxEvent
from src.domain.lead import Lead


class LeadRepository(Protocol):
    async def add(self, lead: Lead) -> None: ...
    async def get_by_id(self, lead_id: UUID) -> Lead | None: ...


class OutboxRepository(Protocol):
    async def add(self, event: OutboxEvent) -> None: ...


class UnitOfWork(Protocol):
    @property
    def leads(self) -> LeadRepository: ...

    @property
    def outbox(self) -> OutboxRepository: ...

    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...
