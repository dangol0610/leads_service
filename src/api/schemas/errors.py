from pydantic import BaseModel


class ErrorDetails(BaseModel):
    """Structured error details."""

    code: str
    message: str
    correlation_id: str


class ErrorResponse(BaseModel):
    """Structured error response wrapper."""

    error: ErrorDetails
