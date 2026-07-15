from fastapi import Request, status
from fastapi.responses import JSONResponse

from src.api.schemas.errors import ErrorDetails, ErrorResponse


async def lead_not_found_handler(request: Request, _: Exception) -> JSONResponse:
    """Handle LeadNotFoundError and return a structured 404 response."""
    correlation_id: str = request.state.correlation_id

    body = ErrorResponse(
        error=ErrorDetails(
            code="lead_not_found",
            message="Заявка не найдена",
            correlation_id=correlation_id,
        )
    )

    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content=body.model_dump(),
    )
