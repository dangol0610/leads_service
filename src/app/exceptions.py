from uuid import UUID


class LeadNotFoundError(Exception):
    def __init__(self, lead_id: UUID) -> None:
        self.lead_id = lead_id
        super().__init__(f"Lead not found: {lead_id}")
