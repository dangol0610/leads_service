from pydantic import BaseModel


class ErrorDetails(BaseModel):
    code: str
    message: str
    correlation_id: str


class ErrorResponse(BaseModel):
    error: ErrorDetails
