from src.app.commands import CreateLeadCommand
from src.app.interfaces import UnitOfWork
from src.domain.events import OutboxEvent
from src.domain.lead import Lead


class CreateLeadService:
    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def execute(self, command: CreateLeadCommand) -> Lead:
        lead = Lead.create(
            name=command.name,
            phone=command.phone,
            source=command.source,
            comment=command.comment,
        )
        event = OutboxEvent.lead_created(lead)

        try:
            await self._uow.leads.add(lead)
            await self._uow.outbox.add(event)
            await self._uow.commit()
        except Exception:
            await self._uow.rollback()
            raise

        return lead
