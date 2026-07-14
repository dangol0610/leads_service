from uuid import UUID

from fastapi import APIRouter, Depends, status

from src.api.dependencies import get_create_lead_service, get_get_lead_service
from src.api.schemas.leads import CreateLeadRequest, LeadResponse
from src.app.commands import CreateLeadCommand, GetLeadQuery
from src.app.services import CreateLeadService, GetLeadService

router = APIRouter(prefix="/leads", tags=["leads"])


@router.post("", status_code=status.HTTP_201_CREATED, response_model=LeadResponse)
async def create_lead(
    request: CreateLeadRequest,
    service: CreateLeadService = Depends(get_create_lead_service),
) -> LeadResponse:
    command = CreateLeadCommand(
        name=request.name,
        phone=str(request.phone),
        source=request.source,
        comment=request.comment,
    )
    lead = await service.execute(command)
    return LeadResponse.from_domain(lead)


@router.get("/{lead_id}", response_model=LeadResponse)
async def get_lead(
    lead_id: UUID,
    service: GetLeadService = Depends(get_get_lead_service),
) -> LeadResponse:
    lead = await service.execute(GetLeadQuery(lead_id=lead_id))
    return LeadResponse.from_domain(lead)
