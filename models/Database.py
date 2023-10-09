from services.ElasticsearchService import ElasticsearchService
from models.Asset import Asset
from fastapi import HTTPException
from uuid import uuid4

from services.Logger import get_logger
logger = get_logger(__name__)

# Initialize an Elasticsearch client
es = ElasticsearchService(hosts=["http://localhost:9200"])

# Create a Pydantic model for the database
class Database(Asset):
    '''
    Pydantic model for a database
    '''
    asset_type: str = "database"

    def create(self, parent_id: str = None):
        '''
        Creates a database and returns it
        '''
        if parent_id is None and self.parent_id is None:
            raise HTTPException(status_code=400, detail=f"Database with id: {self.asset_id} must have a parent domain.")
        if parent_id is None:
            parent_id = self.parent_id
        
        self.asset_id = str(uuid4())
        new_database = self.model_dump()

        domain = self.find_one(asset_id=parent_id, asset_type="domain")

        if domain is None:
            raise HTTPException(status_code=404, detail=f"Domain with id: {parent_id} not found")
        
        for db in domain['children']: # Cannot cast to Domain model because of circular dependency
            if db == self.name:
                raise HTTPException(status_code=400, detail=f"Database with name: {self.name} already exists in domain: {domain['asset_id']}")
    
        domain['children'].append(self.asset_id)  # Cannot cast to Domain model because of circular dependency
        es.insert_new_document(index_name="data_catalog", document_id=self.asset_id, document=new_database)
        es.update_document(document_id=domain['asset_id'], document=domain)
        logger.info(f"Added database: {self.name} to domain: {domain['name']}")
        
        return new_database
    