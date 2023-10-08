from uuid import uuid4
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
    # Create Column for Table
    ###
    create_column = test_client.post(
        "/columns",
        json={
            "name": "test_column", 
            "parent_id": create_table["asset_id"],
            "data_type": "string"
        }
    )
    assert create_column.status_code == 200
    create_column = create_column.json()
    assert create_column["name"] == "test_column"
    assert len(create_column["asset_id"]) == 36
    assert create_column["asset_id"] == create_column["asset_id"]
    assert create_column["parent_id"] == create_table["asset_id"]
    assert create_column["data_type"] == "STRING"
    assert create_column["metadata"] == {}

    ###
    # Search for Column and Ensure Data Type
    ###
    search_column = test_client.get("/columns", params={"asset_id": create_column["asset_id"]})
    assert search_column.status_code == 200
    search_column = search_column.json()
    assert search_column["name"] == "test_column"
    assert len(search_column["asset_id"]) == 36
    assert search_column["asset_id"] == search_column["asset_id"]
    assert search_column["parent_id"] == create_table["asset_id"]
    assert search_column["data_type"] == "STRING"

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
    assert search_table["children"] == [create_column["asset_id"]]

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

def test_ingest_full_source(test_client):
    '''
    Tests the end to end flow of creating a domain and database
    '''
    example_payload = {
        "test_domain": {
            "database1": {
                "table1": {
                    'id': 'integer', 
                    'age': 'integer', 
                    'salary': 'numeric', 
                    'is_active': 'boolean', 
                    'name': 'character varying'
                }
            }
        }
    }

    ingest_full_source = test_client.post("/ingest/full_source", json=example_payload)
    assert ingest_full_source.status_code == 200
    ingest_full_source = ingest_full_source.json()
    assert len(ingest_full_source) == 8 # 1 domain, 1 database, 1 table, 5 columns

    # Domain Test
    assert ingest_full_source[0]["name"] == "test_domain"
    assert ingest_full_source[0]["children"] == []
    assert len(ingest_full_source[0]["asset_id"]) == 36

    # Database Test
    assert ingest_full_source[1]["name"] == "database1"
    assert len(ingest_full_source[1]["asset_id"]) == 36
    assert ingest_full_source[1]["parent_id"] == ingest_full_source[0]["asset_id"]

    # Table Test
    assert ingest_full_source[2]["name"] == "table1"
    assert len(ingest_full_source[2]["asset_id"]) == 36
    assert ingest_full_source[2]["parent_id"] == ingest_full_source[1]["asset_id"]

    # Column Tests
    assert ingest_full_source[3]["name"] == "id"
    assert len(ingest_full_source[3]["asset_id"]) == 36
    assert ingest_full_source[3]["parent_id"] == ingest_full_source[2]["asset_id"]
    assert ingest_full_source[3]["data_type"] == "INTEGER"

    assert ingest_full_source[4]["name"] == "age"
    assert len(ingest_full_source[4]["asset_id"]) == 36
    assert ingest_full_source[4]["parent_id"] == ingest_full_source[2]["asset_id"]
    assert ingest_full_source[4]["data_type"] == "INTEGER"

    assert ingest_full_source[5]["name"] == "salary"
    assert len(ingest_full_source[5]["asset_id"]) == 36
    assert ingest_full_source[5]["parent_id"] == ingest_full_source[2]["asset_id"]
    assert ingest_full_source[5]["data_type"] == "NUMERIC"

    assert ingest_full_source[6]["name"] == "is_active"
    assert len(ingest_full_source[6]["asset_id"]) == 36
    assert ingest_full_source[6]["parent_id"] == ingest_full_source[2]["asset_id"]
    assert ingest_full_source[6]["data_type"] == "BOOLEAN"

    assert ingest_full_source[7]["name"] == "name"
    assert len(ingest_full_source[7]["asset_id"]) == 36
    assert ingest_full_source[7]["parent_id"] == ingest_full_source[2]["asset_id"]
    assert ingest_full_source[7]["data_type"] == "CHARACTER VARYING"

def test_search_names(test_client):
    '''
    Tests the ability to search based on names and getting documents back
    '''
    prefix = str(uuid4().hex[0:6])
    example_payload = {
        f"{prefix}1": {
            f"{prefix}12": {
                f"{prefix}123": {
                    f"{prefix}1234": 'integer', 
                    f"{prefix}12345": 'integer', 
                    f"{prefix}123456": 'numeric', 
                    f"{prefix}1234567": 'boolean', 
                    f"{prefix}12345678": 'character varying'
                }
            }
        }
    }

    ingest_full_source = test_client.post("/ingest/full_source", json=example_payload)
    assert ingest_full_source.status_code == 200

    suffix = ""
    for i in range(1,9):
        suffix += str(i)
        search_names = test_client.get("/search/names", params={"value": f"{prefix}{suffix}"})
        assert search_names.status_code == 200
        search_names = search_names.json()
        assert len(search_names) == 9-i

