from services.ElasticsearchService import ElasticsearchService
from models.Asset import Asset
from fastapi import HTTPException
from uuid import uuid4

from services.Logger import get_logger
logger = get_logger(__name__)

# Initialize an Elasticsearch client
es = ElasticsearchService(hosts=["http://localhost:9200"])

# Create a Pydantic model for the domain
class Database(Asset):
    '''
    Pydantic model for a database
    '''
    asset_type: str = "database"

    def create(self, parent_id: str):
        '''
        Creates a database and returns it
        '''
        if parent_id is None:
            raise HTTPException(status_code=400, detail=f"Database with id: {self.asset_id} must have a parent domain.")
        
        self.asset_id = str(uuid4())
        new_database = self.model_dump()

        domain = self.search(asset_id=parent_id)

        if len(domain) == 0:
            raise HTTPException(status_code=404, detail=f"Domain with id: {parent_id} not found")
        elif len(domain) == 1:
            domain = domain[0]
            for db in domain['databases']: # Cannot cast to Domain model because of circular dependency
                if db['name'] == self.name:
                    raise HTTPException(status_code=400, detail=f"Database with name: {self.name} already exists in domain: {domain['asset_id']}")
        else:
            raise HTTPException(status_code=500, detail=f"Found multiple domains with id: {self.asset_id}")
    
        domain['children'].append(new_database)  # Cannot cast to Domain model because of circular dependency
        logger.info(domain)
        es.update_document(document_id=self.asset_id, document=domain)
        logger.info(f"Added database: {self.name} to domain: {domain['name']}")

        return new_database
    