import app
import asyncio
from services.ElasticsearchService import ElasticsearchService

from models.Database import Database
from models.Domain import Domain

import pytest

from fastapi.testclient import TestClient

@pytest.fixture(autouse=True)
def ensure_elasticsearch():
    es = ElasticsearchService(hosts=["http://localhost:9200"])
    yield es
    es.delete_index()

def test_create_domain_database():
    client = TestClient(app.app)
    response = client.post("/domains", json={"name": "test_domain"})
    assert response.status_code == 200
    response = response.json()
    assert response["name"] == "test_domain"
    assert response["databases"] == []
    assert len(response["domain_id"]) == 36

    response = client.post("/databases", json={"name": "test_database", "domain_id": response["domain_id"]})
    assert response.status_code == 200
    response = response.json()
    assert response["name"] == "test_database"
    assert len(response["database_id"]) == 36
    assert response["domain_id"] == response["domain_id"]