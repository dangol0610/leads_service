from contextlib import asynccontextmanager

from fastapi import FastAPI

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
    app.state.database = database
    yield
    await database.close()


app = FastAPI(
    title="Leads Service",
    version="1.0.0",
    description="Leads Service API",
    lifespan=lifespan,
)
