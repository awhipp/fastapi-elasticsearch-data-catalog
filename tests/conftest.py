import os

os.environ['WAIT_FOR_ELASTICSEARCH'] = 'true'

import app
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
