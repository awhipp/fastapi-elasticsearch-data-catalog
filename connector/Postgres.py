from connector.Connector import Connector
import psycopg2
from typing import Any

class PostgresConnector(Connector):
    name: str = "Postgres"
    connection: Any = None
    cursor: Any = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if kwargs['params'] is None:
            raise ValueError("Params cannot be None")
        
        self.params = kwargs['params']
        if 'host' not in self.params:
            raise ValueError("Host cannot be None")
        if 'port' not in self.params:
            raise ValueError("Port cannot be None")
        if 'database' not in self.params:
            self.params["database"] = "postgres" # Default to postgres database
        if 'user' not in self.params:
            raise ValueError("User cannot be None")
        if 'password' not in self.params:
            raise ValueError("Password cannot be None")
        
        self.connect()
        

    def connect(self) -> tuple[Any, Any]:
        '''
        Connects to the data source
        '''
        try:
            self.connection = psycopg2.connect(**self.params)
            self.cursor = self.connection.cursor()
        except psycopg2.Error as e:
            print("Error connecting to the database:", e)
            

    def close(self):
        '''
        Closes the connection to the data source
        '''
        self.connection.close()
        self.cursor.close()
    
    def process(self):
        '''
        Connects to the data source, and possibly processes the source into catalog
        '''

        databases = self.get_databases()

        for database in databases:
            if database == "postgres":
                continue
            self.params["database"] = database
            self.connect()

            tables = self.get_tables(database=database)
            for table in tables:
                self.get_columns(database=database, table=table)
        
            self.close()

    def get_databases(self):
        '''
        Gets all databases from the data source
        '''
        self.connect()

        self.cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        databases = [record[0] for record in self.cursor.fetchall()]
        for database in databases:
            if database == "postgres":
                continue
            self.catalog[database] = {}

        self.close()

        return databases
    
    def get_tables(self, database: str):
        '''
        Gets all tables from a database
        '''
        self.cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        tables = [record[0] for record in self.cursor.fetchall()]

        for table in tables:
            self.catalog[database][table] = {}

        return tables

    def get_columns(self, database: str, table: str):
        '''
        Gets all columns from a table
        '''
        self.cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}';")
        columns = self.cursor.fetchall()
        for column in columns:
            self.catalog[database][table][column[0]] = column[1]