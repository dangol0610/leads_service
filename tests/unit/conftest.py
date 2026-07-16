from uuid import UUID

import pytest

from src.app.interfaces import (
    InboundEventRepository,
    LeadRepository,
    MessageProducer,
    OutboxRepository,
)
from src.domain.events import InboundEvent, OutboxEvent
from src.domain.lead import Lead, LeadStatus


class MockLeadRepository(LeadRepository):
    def __init__(self) -> None:
        self._leads: dict[UUID, Lead] = {}

    async def add(self, lead: Lead) -> None:
        self._leads[lead.id] = lead

    async def get_by_id(self, lead_id: UUID) -> Lead | None:
        return self._leads.get(lead_id)

    async def update_status(self, lead_id: UUID, status: LeadStatus) -> None:
        lead = self._leads.get(lead_id)
        if lead is not None:
            lead.status = status


class MockOutboxRepository(OutboxRepository):
    def __init__(self) -> None:
        self._events: list[OutboxEvent] = []

    async def add(self, event: OutboxEvent) -> None:
        self._events.append(event)

    async def get_unpublished(self) -> list[OutboxEvent]:
        return [e for e in self._events if e.published_at is None]

    async def mark_as_published(self, event_id: UUID) -> None:
        for event in self._events:
            if event.event_id == event_id:
                event.mark_published()


class MockInboundEventRepository(InboundEventRepository):
    def __init__(self) -> None:
        self._events: set[UUID] = set()

    async def exists(self, event_id: UUID) -> bool:
        return event_id in self._events

    async def add(self, event: InboundEvent) -> None:
        self._events.add(event.event_id)


class MockUnitOfWork:
    def __init__(self) -> None:
        self.leads = MockLeadRepository()
        self.outbox = MockOutboxRepository()
        self.inbound = MockInboundEventRepository()
        self.committed = False
        self.rolled_back = False

    async def commit(self) -> None:
        self.committed = True

    async def rollback(self) -> None:
        self.rolled_back = True


class MockProducer(MessageProducer):
    def __init__(self) -> None:
        self.sent: list[tuple[str, str, bytes]] = []
        self.started = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

    async def send(self, topic: str, key: str, value: bytes) -> None:
        self.sent.append((topic, key, value))


@pytest.fixture
def mock_uow() -> MockUnitOfWork:
    return MockUnitOfWork()


@pytest.fixture
def mock_producer() -> MockProducer:
    return MockProducer()


@pytest.fixture
def sample_lead() -> Lead:
    return Lead.create(
        name="Иван",
        phone="+79991234567",
        source="landing",
        comment="Хочу консультацию",
    )
