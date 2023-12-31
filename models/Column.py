from services.ElasticsearchService import ElasticsearchService
from models.Asset import Asset
from fastapi import HTTPException
from uuid import uuid4

from services.Logger import get_logger
logger = get_logger(__name__)

# Initialize an Elasticsearch client
es = ElasticsearchService(hosts=["http://localhost:9200"])

# Create a Pydantic model for the column
class Column(Asset):
    '''
    Pydantic model for a table
    '''
    asset_type: str = "column"
    data_type: str = None
    metadata: dict = {}

    def create(self, parent_id: str = None, d_type: str = None):
        '''
        Creates a table and returns it
        '''
        if parent_id is None and self.parent_id is None:
            raise HTTPException(status_code=400, detail=f"Column with id: {self.asset_id} must have a parent table.")
        if parent_id is None:
            parent_id = self.parent_id
        
        if d_type is None and self.data_type is None:
            raise HTTPException(status_code=400, detail=f"Column with id: {self.asset_id} must have a data type.")
        
        if d_type != None:
            self.data_type = d_type.upper()
        else:
            self.data_type = self.data_type.upper()
        
        self.asset_id = str(uuid4())
        new_column = self.model_dump()

        table = self.find_one(asset_id=parent_id, asset_type="table")

        if table is None:
            raise HTTPException(status_code=404, detail=f"Table with id: {parent_id} not found")
        
        for column_id in table['children']: # Cannot cast to Domain model because of circular dependency
            if column_id == self.name:
                raise HTTPException(status_code=400, detail=f"Column with name: {self.name} already exists in Table: {table['asset_id']}")
    
        table['children'].append(self.asset_id)  # Cannot cast to Domain model because of circular dependency
        es.insert_new_document(index_name="data_catalog", document_id=self.asset_id, document=new_column)
        es.update_document(document_id=table['asset_id'], document=table)
        logger.info(f"Added column: {self.name} to table: {table['name']}")
        
        return new_column
    