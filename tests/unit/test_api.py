from uuid import uuid4

import pytest
from fastapi import status
from httpx import ASGITransport, AsyncClient

from src.api.dependencies import get_create_lead_service, get_get_lead_service
from src.main import app
from tests.unit.conftest import MockUnitOfWork


@pytest.fixture
def mock_uow() -> MockUnitOfWork:
    return MockUnitOfWork()


@pytest.fixture
def override_deps(mock_uow: MockUnitOfWork) -> None:
    app.dependency_overrides.clear()

    async def _mock_create():
        from src.app.services import CreateLeadService

        return CreateLeadService(mock_uow)

    async def _mock_get():
        from src.app.services import GetLeadService

        return GetLeadService(mock_uow)

    app.dependency_overrides[get_create_lead_service] = _mock_create
    app.dependency_overrides[get_get_lead_service] = _mock_get


@pytest.mark.asyncio
async def test_create_lead(override_deps: None, mock_uow: MockUnitOfWork) -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport, base_url="http://test/api/v1"
    ) as client:
        response = await client.post(
            "/leads",
            json={
                "name": "Иван",
                "phone": "+79991234567",
                "source": "landing",
                "comment": "test",
            },
        )

    assert response.status_code == status.HTTP_201_CREATED
    data = response.json()
    assert data["name"] == "Иван"
    assert data["phone"] == "tel:+7-999-123-45-67"
    assert data["source"] == "landing"
    assert data["comment"] == "test"
    assert data["status"] == "new"
    assert mock_uow.committed is True


@pytest.mark.asyncio
async def test_get_lead_returns_404(
    override_deps: None, mock_uow: MockUnitOfWork
) -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport, base_url="http://test/api/v1"
    ) as client:
        response = await client.get(f"/leads/{uuid4()}")

    assert response.status_code == status.HTTP_404_NOT_FOUND
    data = response.json()
    assert data["error"]["code"] == "lead_not_found"
    assert "correlation_id" in data["error"]
