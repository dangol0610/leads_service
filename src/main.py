from contextlib import asynccontextmanager
from typing import Callable
from uuid import uuid4

from fastapi import FastAPI, Request, Response

from src.api.exception_handlers import lead_not_found_handler
from src.api.routes.leads import router as leads_router
from src.app.exceptions import LeadNotFoundError
from src.core.config import settings
from src.infrastructure.database.config import Database

database = Database(
    database_url=settings.database_url,
    pool_size=settings.DB_POOL_SIZE,
    max_overflow=settings.DB_MAX_OVERFLOW,
    pool_pre_ping=settings.DB_POOL_PRE_PING,
    pool_recycle=settings.DB_POOL_RECYCLE,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: initialize database on start, dispose on shutdown."""
    app.state.database = database
    yield
    await database.close()


app = FastAPI(
    title="Leads Service",
    version="1.0.0",
    description="Leads Service API",
    lifespan=lifespan,
)

app.include_router(leads_router, prefix="/api/v1")


@app.middleware("http")
async def add_correlation_id(request: Request, call_next: Callable) -> Response:
    """Add or propagate X-Correlation-ID header for request tracing."""
    correlation_id = request.headers.get("X-Correlation-ID") or str(uuid4())
    request.state.correlation_id = correlation_id

    response = await call_next(request)
    response.headers["X-Correlation-ID"] = correlation_id

    return response


app.add_exception_handler(
    LeadNotFoundError,
    lead_not_found_handler,
)
