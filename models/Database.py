from services.ElasticsearchService import ElasticsearchService
from pydantic import BaseModel, UUID4
from fastapi import HTTPException
from uuid import uuid4

from utilities.DomainHelpers import search as domain_search

from services.Logger import get_logger
logger = get_logger(__name__)

# Initialize an Elasticsearch client
es = ElasticsearchService(hosts=["http://localhost:9200"])

# Create a Pydantic model for the domain
class Database(BaseModel):
    '''
    Pydantic model for a domain
    '''
    name: str
    domain_id: UUID4
    database_id: UUID4 = None

    def create_database(self):
        '''
        Creates a domain and returns it
        '''
        self.database_id = uuid4()
        new_database = self.model_dump()

        domain = domain_search(domain_id=self.domain_id)

        if len(domain) == 0:
            raise HTTPException(status_code=404, detail=f"Domain with id: {self.domain_id} not found")
        elif len(domain) == 1:
            domain = domain[0]
            for db in domain['databases']: # Cannot cast to Domain model because of circular dependency
                if db['name'] == self.name:
                    raise HTTPException(status_code=400, detail=f"Database with name: {self.name} already exists in domain: {domain.domain_id}")
        else:
            raise HTTPException(status_code=500, detail=f"Found multiple domains with id: {self.domain_id}")
    
        domain['databases'].append(new_database)  # Cannot cast to Domain model because of circular dependency
        logger.info(domain)
        es.update_document(document_id=self.domain_id, document=domain)
        logger.info(f"Added database: {self.name} to domain: {domain['name']}")

        return new_database
    