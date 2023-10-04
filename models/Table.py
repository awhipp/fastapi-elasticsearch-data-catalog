from services.ElasticsearchService import ElasticsearchService
from models.Asset import Asset
from fastapi import HTTPException
from uuid import uuid4

from services.Logger import get_logger
logger = get_logger(__name__)

# Initialize an Elasticsearch client
es = ElasticsearchService(hosts=["http://localhost:9200"])

# Create a Pydantic model for the Table
class Table(Asset):
    '''
    Pydantic model for a table
    '''
    asset_type: str = "table"

    def create(self, parent_id: str):
        '''
        Creates a table and returns it
        '''
        if parent_id is None:
            raise HTTPException(status_code=400, detail=f"Table with id: {self.asset_id} must have a parent database.")
        
        self.asset_id = str(uuid4())
        new_table = self.model_dump()

        database = self.find_one(asset_id=parent_id, asset_type="database")

        if database is None:
            raise HTTPException(status_code=404, detail=f"Database with id: {parent_id} not found")
        
        for table in database['children']: # Cannot cast to Domain model because of circular dependency
            if table['name'] == self.name:
                raise HTTPException(status_code=400, detail=f"Table with name: {self.name} already exists in database: {database['asset_id']}")
    
        database['children'].append(self.asset_id)  # Cannot cast to Domain model because of circular dependency
        es.insert_new_document(index_name="data_catalog", document_id=self.asset_id, document=new_table)
        es.update_document(document_id=database['asset_id'], document=database)
        logger.info(f"Added table: {self.name} to database: {database['name']}")
        
        return new_table
    