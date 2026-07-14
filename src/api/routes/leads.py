from fastapi import APIRouter, Depends, status

from src.api.dependencies import get_create_lead_service
from src.api.schemas.leads import CreateLeadRequest, LeadResponse
from src.app.commands import CreateLeadCommand
from src.app.services import CreateLeadService

router = APIRouter(prefix="/leads", tags=["leads"])


@router.post("/", status_code=status.HTTP_201_CREATED, response_model=LeadResponse)
async def create_lead(
    request: CreateLeadRequest,
    service: CreateLeadService = Depends(get_create_lead_service),
) -> LeadResponse:
    command = CreateLeadCommand(
        name=request.name,
        phone=request.phone,
        source=request.source,
        comment=request.comment,
    )
    lead = await service.execute(command)
    return LeadResponse.from_domain(lead)
