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

def test_e2e(test_client):
    '''
    Tests the end to end flow of creating a domain and database
    '''

    ###
    # Create Domain
    ###
    create_domain = test_client.post("/domains", json={"name": "test_domain"})
    assert create_domain.status_code == 200
    create_domain = create_domain.json()
    assert create_domain["name"] == "test_domain"
    assert create_domain["children"] == []
    assert len(create_domain["asset_id"]) == 36

    ###
    # Create Database for Domain
    ###
    create_database = test_client.post(
        "/databases",
        json={
            "name": "test_database", 
            "parent_id": create_domain["asset_id"]
        }
    )
    assert create_database.status_code == 200
    create_database = create_database.json()
    assert create_database["name"] == "test_database"
    assert len(create_database["asset_id"]) == 36
    assert create_database["asset_id"] == create_database["asset_id"]
    assert create_database["parent_id"] == create_domain["asset_id"]
    
    ###
    # Create Table for Database
    ###
    create_table = test_client.post(
        "/tables",
        json={
            "name": "test_table", 
            "parent_id": create_database["asset_id"]
        }
    )
    assert create_table.status_code == 200
    create_table = create_table.json()
    assert create_table["name"] == "test_table"
    assert len(create_table["asset_id"]) == 36
    assert create_table["asset_id"] == create_table["asset_id"]
    assert create_table["parent_id"] == create_database["asset_id"]

    ###
    # Search for Table and Ensure was Updated
    ###
    search_table = test_client.get("/tables", params={"asset_id": create_table["asset_id"]})
    assert search_table.status_code == 200
    search_table = search_table.json()
    assert search_table["name"] == "test_table"
    assert len(search_table["asset_id"]) == 36
    assert search_table["asset_id"] == search_table["asset_id"]
    assert search_table["parent_id"] == create_database["asset_id"]
    assert search_table["children"] == []

    ###
    # Search for Database and Ensure was Updated
    ###
    search_database = test_client.get("/databases", params={"asset_id": create_database["asset_id"]})
    assert search_database.status_code == 200
    search_database = search_database.json()
    assert search_database["name"] == "test_database"
    assert len(search_database["asset_id"]) == 36
    assert search_database["asset_id"] == search_database["asset_id"]
    assert search_database["parent_id"] == create_domain["asset_id"]
    assert search_database["children"] == [create_table["asset_id"]]

    ###
    # Search for Domain and Ensure was Updated
    ###
    search_domain = test_client.get("/domains", params={"asset_id": create_domain["asset_id"]})
    assert search_domain.status_code == 200
    search_domain = search_domain.json()
    assert search_domain["name"] == "test_domain"
    assert search_domain["children"] == [search_database["asset_id"]]
    assert len(search_domain["asset_id"]) == 36