from datetime import UTC, datetime

from uuid import uuid4

from src.domain.events import InboundEvent, OutboxEvent
from src.domain.lead import Lead, LeadStatus


class TestLead:
    def test_create_sets_status_new(self) -> None:
        lead = Lead.create(name="Test", phone="+70000000000", source="test", comment=None)

        assert lead.status == LeadStatus.NEW
        assert lead.name == "Test"
        assert lead.phone == "+70000000000"
        assert lead.source == "test"
        assert lead.comment is None

    def test_create_sets_timestamps(self) -> None:
        lead = Lead.create(name="Test", phone="+70000000000", source="test", comment="c")

        assert isinstance(lead.created_at, datetime)
        assert isinstance(lead.updated_at, datetime)
        assert lead.created_at == lead.updated_at

    def test_apply_moderation_approved(self) -> None:
        lead = Lead.create(name="Test", phone="+70000000000", source="test", comment=None)

        lead.apply_moderation(approved=True)

        assert lead.status == LeadStatus.APPROVED

    def test_apply_moderation_rejected(self) -> None:
        lead = Lead.create(name="Test", phone="+70000000000", source="test", comment=None)

        lead.apply_moderation(approved=False)

        assert lead.status == LeadStatus.REJECTED

    def test_apply_moderation_updates_timestamp(self) -> None:
        lead = Lead.create(name="Test", phone="+70000000000", source="test", comment=None)
        original = lead.updated_at

        lead.apply_moderation(approved=True)

        assert lead.updated_at > original

    def test_lead_id_is_uuid(self) -> None:
        lead = Lead.create(name="Test", phone="+70000000000", source="test", comment=None)

        assert lead.id is not None
        assert isinstance(lead.id, type(lead.id))


class TestOutboxEvent:
    def test_lead_created_creates_event(self, sample_lead: Lead) -> None:
        event = OutboxEvent.lead_created(sample_lead)

        assert event.event_type == "lead_created.v1"
        assert event.aggregate_id == sample_lead.id
        assert event.published_at is None
        assert event.payload["lead_id"] == str(sample_lead.id)

    def test_mark_published_sets_timestamp(self) -> None:
        lead = Lead.create(name="Test", phone="+70000000000", source="test", comment=None)
        event = OutboxEvent.lead_created(lead)

        event.mark_published()

        assert event.published_at is not None
        assert isinstance(event.published_at, datetime)


class TestInboundEvent:
    def test_creates_event(self) -> None:
        event = InboundEvent(
            event_id=uuid4(),
            event_type="lead_moderation_finished.v1",
            aggregate_id=uuid4(),
            payload={"approved": True},
            received_at=datetime.now(UTC),
        )

        assert event.event_type == "lead_moderation_finished.v1"
        assert event.received_at is not None

    def test_received_at_defaults_to_none(self) -> None:
        event = InboundEvent(
            event_id=uuid4(),
            event_type="lead_moderation_finished.v1",
            aggregate_id=uuid4(),
            payload={},
        )

        assert event.received_at is None
