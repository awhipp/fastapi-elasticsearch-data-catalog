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
    domain_response = test_client.post("/domains", json={"name": "test_domain"})
    assert domain_response.status_code == 200
    domain_response = domain_response.json()
    assert domain_response["name"] == "test_domain"
    assert domain_response["children"] == []
    assert len(domain_response["asset_id"]) == 36

    time.sleep(1) # Wait for Elasticsearch to index the document

    database_response = test_client.post(
        "/databases",
        json={
            "name": "test_database", 
            "parent_id": domain_response["asset_id"]
        }
    )
    assert database_response.status_code == 200
    database_response = database_response.json()
    assert database_response["name"] == "test_database"
    assert len(database_response["database_id"]) == 36
    assert database_response["asset_id"] == database_response["domain_id"]