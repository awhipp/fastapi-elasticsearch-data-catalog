import app
import time
from services.ElasticsearchService import ElasticsearchService

import pytest

from fastapi.testclient import TestClient

@pytest.fixture(autouse=True)
def ensure_elasticsearch():
    es = ElasticsearchService(hosts=["http://localhost:9200"])
    yield es
    es.delete_index()

@pytest.fixture()
def test_client():
    client = TestClient(app.app)
    yield client

def test_e2e(test_client):
    response = test_client.post("/domains", json={"name": "test_domain"})
    assert response.status_code == 200
    response = response.json()
    assert response["name"] == "test_domain"
    assert response["databases"] == []
    assert len(response["domain_id"]) == 36

    time.sleep(1) # Wait for Elasticsearch to index the document

    response = test_client.post("/databases", json={"name": "test_database", "domain_id": response["domain_id"]})
    assert response.status_code == 200
    response = response.json()
    assert response["name"] == "test_database"
    assert len(response["database_id"]) == 36
    assert response["domain_id"] == response["domain_id"]