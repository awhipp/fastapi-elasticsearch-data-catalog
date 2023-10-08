from connector.Postgres import PostgresConnector

def test_postgres(test_client):
    pgConnector = PostgresConnector(params={
        "host": "127.0.0.1",
        "port": "5432",
        "user": "myuser",
        "password": "mypassword"
    })

    pgConnector.process()

    expected_result = {
        "database1": {
            "table1": {
                'id': 'integer', 
                'age': 'integer', 
                'salary': 'numeric', 
                'is_active': 'boolean', 
                'name': 'character varying'
            }
        },
        "database2": {
            "table2": {
                'product_id': 'integer', 
                'price': 'numeric', 
                'purchase_date': 'date',
                'product_name': 'character varying'
            }
        }
    }

    assert pgConnector.catalog == expected_result
