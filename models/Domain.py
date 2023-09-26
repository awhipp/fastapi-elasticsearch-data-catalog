from services.ElasticsearchService import ElasticsearchService
from pydantic import BaseModel, UUID4
from fastapi import HTTPException
from uuid import uuid4

from models.Database import Database

from services.Logger import get_logger
logger = get_logger(__name__)


# Initialize an Elasticsearch client
es = ElasticsearchService(hosts=["http://localhost:9200"])

# Create a Pydantic model for the domain
class Domain(BaseModel):
    '''
    Pydantic model for a domain
    '''
    name: str
    id: UUID4 = None
    databases: list[Database] = []

    def create_domain(self):
        '''
        Creates a domain and returns it
        '''
        self.id = uuid4()
        new_domain = self.model_dump()

        if len(self.search(self.name)) > 0:
            raise HTTPException(status_code=400, detail=f"Domain with name: {self.name} already exists")
        else:
            es.insert_new_document(index_name="data_catalog", document_id=self.id, document=new_domain)

        return new_domain
    
    def add(self):
        '''
        Adds a domain to Elasticsearch
        '''
        es.client.index(
            index="data_catalog",
            id=self.id,
            document=self.model_dump()
        )
        logger.info(f"Added domain: {self.name}")
        return self
    
    @staticmethod
    def search(id: str):
        '''
        Searches for a domain by name and returns it
        '''
        query = {
            "match": {
                "id": id
            }
        }
        results = es.search(index_name="data_catalog", query=query)
        logger.info(f"Found {len(results)} domains with name: {id}")
        return results
