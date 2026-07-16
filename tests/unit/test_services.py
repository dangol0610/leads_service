from datetime import UTC, datetime
from uuid import uuid4

import pytest

from src.app.commands import (
    CreateLeadCommand,
    GetLeadQuery,
    ProcessModerationCommand,
)
from src.app.exceptions import LeadNotFoundError
from src.app.services import (
    CreateLeadService,
    GetLeadService,
    ModerationConsumerService,
    OutboxPublisherService,
)
from src.domain.events import OutboxEvent
from src.domain.lead import Lead, LeadStatus
from tests.unit.conftest import MockProducer, MockUnitOfWork


class TestCreateLeadService:
    @pytest.mark.asyncio
    async def test_creates_lead_and_outbox_event(
        self, mock_uow: MockUnitOfWork
    ) -> None:
        service = CreateLeadService(mock_uow)
        command = CreateLeadCommand(
            name="Test", phone="+70000000000", source="test", comment="c"
        )

        lead = await service.execute(command)

        assert lead.name == "Test"
        assert lead.status == LeadStatus.NEW
        assert mock_uow.committed is True
        assert len(mock_uow.outbox._events) == 1

    @pytest.mark.asyncio
    async def test_rollback_on_error(self, mock_uow: MockUnitOfWork) -> None:
        service = CreateLeadService(mock_uow)
        command = CreateLeadCommand(
            name="", phone="+70000000000", source="test", comment="c"
        )

        mock_uow.leads.add = None  # type: ignore

        with pytest.raises(TypeError):
            await service.execute(command)

        assert mock_uow.rolled_back is True


class TestGetLeadService:
    @pytest.mark.asyncio
    async def test_returns_lead_when_found(
        self, mock_uow: MockUnitOfWork, sample_lead: Lead
    ) -> None:
        await mock_uow.leads.add(sample_lead)
        service = GetLeadService(mock_uow)

        lead = await service.execute(GetLeadQuery(lead_id=sample_lead.id))

        assert lead.id == sample_lead.id

    @pytest.mark.asyncio
    async def test_raises_error_when_not_found(self, mock_uow: MockUnitOfWork) -> None:
        service = GetLeadService(mock_uow)

        with pytest.raises(LeadNotFoundError):
            await service.execute(GetLeadQuery(lead_id=uuid4()))


class TestOutboxPublisherService:
    @pytest.mark.asyncio
    async def test_publishes_unpublished_events(
        self, mock_uow: MockUnitOfWork, mock_producer: MockProducer
    ) -> None:
        lead = Lead.create(
            name="Test", phone="+70000000000", source="test", comment=None
        )
        await mock_uow.leads.add(lead)
        event = OutboxEvent(
            event_id=uuid4(),
            event_type="lead_created.v1",
            aggregate_id=lead.id,
            payload={},
            occurred_at=datetime.now(UTC),
        )
        mock_uow.outbox._events = [event]
        service = OutboxPublisherService(mock_uow, mock_producer)

        count = await service.execute(topic="test-topic")

        assert count == 1
        assert len(mock_producer.sent) == 1
        assert mock_producer.sent[0][0] == "test-topic"
        assert mock_uow.committed is True

    @pytest.mark.asyncio
    async def test_returns_zero_when_no_events(
        self, mock_uow: MockUnitOfWork, mock_producer: MockProducer
    ) -> None:
        service = OutboxPublisherService(mock_uow, mock_producer)

        count = await service.execute(topic="test-topic")

        assert count == 0
        assert len(mock_producer.sent) == 0

    @pytest.mark.asyncio
    async def test_rollback_on_failure(
        self, mock_uow: MockUnitOfWork, mock_producer: MockProducer
    ) -> None:
        lead = Lead.create(
            name="Test", phone="+70000000000", source="test", comment=None
        )
        await mock_uow.leads.add(lead)
        event = OutboxEvent(
            event_id=uuid4(),
            event_type="lead_created.v1",
            aggregate_id=lead.id,
            payload={},
            occurred_at=datetime.now(UTC),
        )
        mock_uow.outbox._events = [event]

        mock_producer.send = None  # type: ignore
        service = OutboxPublisherService(mock_uow, mock_producer)

        with pytest.raises(TypeError):
            await service.execute(topic="test-topic")

        assert mock_uow.rolled_back is True


class TestModerationConsumerService:
    @pytest.mark.asyncio
    async def test_approves_lead(
        self, mock_uow: MockUnitOfWork, sample_lead: Lead
    ) -> None:
        await mock_uow.leads.add(sample_lead)
        service = ModerationConsumerService(mock_uow)
        command = ProcessModerationCommand(
            event_id=uuid4(),
            event_type="lead_moderation_finished.v1",
            aggregate_id=sample_lead.id,
            payload={"approved": True},
        )

        result = await service.process(command)

        assert result is True
        lead = await mock_uow.leads.get_by_id(sample_lead.id)
        assert lead is not None
        assert lead.status == LeadStatus.APPROVED
        assert mock_uow.committed is True

    @pytest.mark.asyncio
    async def test_rejects_lead(
        self, mock_uow: MockUnitOfWork, sample_lead: Lead
    ) -> None:
        await mock_uow.leads.add(sample_lead)
        service = ModerationConsumerService(mock_uow)
        command = ProcessModerationCommand(
            event_id=uuid4(),
            event_type="lead_moderation_finished.v1",
            aggregate_id=sample_lead.id,
            payload={"approved": False},
        )

        result = await service.process(command)

        assert result is True
        lead = await mock_uow.leads.get_by_id(sample_lead.id)
        assert lead is not None
        assert lead.status == LeadStatus.REJECTED

    @pytest.mark.asyncio
    async def test_skips_duplicate_event(
        self, mock_uow: MockUnitOfWork, sample_lead: Lead
    ) -> None:
        await mock_uow.leads.add(sample_lead)
        service = ModerationConsumerService(mock_uow)
        event_id = uuid4()
        command = ProcessModerationCommand(
            event_id=event_id,
            event_type="lead_moderation_finished.v1",
            aggregate_id=sample_lead.id,
            payload={"approved": True},
        )

        await service.process(command)
        result = await service.process(command)

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_lead_not_found(
        self, mock_uow: MockUnitOfWork
    ) -> None:
        service = ModerationConsumerService(mock_uow)
        command = ProcessModerationCommand(
            event_id=uuid4(),
            event_type="lead_moderation_finished.v1",
            aggregate_id=uuid4(),
            payload={"approved": True},
        )

        result = await service.process(command)

        assert result is False

    @pytest.mark.asyncio
    async def test_rollback_on_error(
        self, mock_uow: MockUnitOfWork, sample_lead: Lead
    ) -> None:
        await mock_uow.leads.add(sample_lead)
        service = ModerationConsumerService(mock_uow)
        command = ProcessModerationCommand(
            event_id=uuid4(),
            event_type="lead_moderation_finished.v1",
            aggregate_id=sample_lead.id,
            payload={"approved": True},
        )

        mock_uow.inbound.add = None  # type: ignore

        with pytest.raises(TypeError):
            await service.process(command)

        assert mock_uow.rolled_back is True
