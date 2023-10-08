import os

os.environ['WAIT_FOR_ELASTICSEARCH'] = 'true'

import app
from services.ElasticsearchService import ElasticsearchService

import pytest

from fastapi.testclient import TestClient

@pytest.fixture()
def ensure_elasticsearch():
    es = ElasticsearchService(hosts=["http://localhost:9200"])
    yield es
    es.remove_all_documents()

@pytest.fixture()
def test_client(ensure_elasticsearch):
    '''
    Yields a test FastAPI client and deletes the Elasticsearch index after the test is complete
    '''
    client = TestClient(app.app)
    yield client
