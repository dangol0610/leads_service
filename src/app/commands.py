from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CreateLeadCommand:
    name: str
    phone: str
    source: str
    comment: str
