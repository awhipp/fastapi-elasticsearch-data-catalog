from pydantic import BaseModel
from typing import Any

class Connector(BaseModel, arbitrary_types_allowed=True):
    '''
    Superclass for all connectors to inherit from.

    Connectors allow the user to connect to a data source and pull catalog information for ingestion.
    '''

    name: str
    params: dict = {}
    catalog: dict = {}

    def __new__(cls, *args, **kwargs):
        if cls is Connector:
            raise TypeError("Cannot instantiate Connector directly, use a subclass instead")
        return super().__new__(cls)

    def __init__(self, *args, **kwargs):
        # Check if class is Connector
        if self.__class__ is Connector:
            raise TypeError("Cannot instantiate Connector directly, use a subclass instead")

        super().__init__(*args, **kwargs)


    def ingest(self):
        '''
        Ingests the catalog information into the catalog database
        '''
        for database in self.catalog:
            for table in self.catalog[database]:
                for column in self.catalog[database][table]:
                    print(f"{database}.{table}.{column}: {self.catalog[database][table][column]}")

    def connect(self) -> Any:
        '''
        Connects to the data source
        '''
        raise NotImplementedError
    
    def close(self):
        '''
        Closes the connection to the data source
        '''
        raise NotImplementedError
    
    def process(self) -> Any:
        '''
        Connects to the data source, and possibly processes the source into catalog
        '''
        raise NotImplementedError