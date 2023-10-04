from services.ElasticsearchService import ElasticsearchService
from pydantic import BaseModel, UUID4
from fastapi import HTTPException
from uuid import uuid4
from typing import Optional

from services.Logger import get_logger
logger = get_logger(__name__)


# Initialize an Elasticsearch client
es = ElasticsearchService(hosts=["http://localhost:9200"])

# Create a Pydantic model for the domain
class Asset(BaseModel):
    '''
    Superclass model for an asset
    '''
    name: str
    asset_type: Optional[str] = None
    asset_id: str = None # Also the document id in Elasticsearch
    parent_id: str = None # None for domains
    children: list = []

    def __new__(cls, *args, **kwargs):
        if cls is Asset:
            raise TypeError("Cannot instantiate Asset directly, use a subclass instead")
        return super().__new__(cls)

    def create(self, parent_id=None):
        '''
        Creates an asset and returns it
        '''
        self.asset_id = str(uuid4())
        new_asset = self.model_dump()

        if parent_id is not None:
            raise HTTPException(status_code=400, detail=f"Asset with id: {self.asset_id} cannot have a parent")

        # Broad create, each subclass will have its own create method if checking is needed
        es.insert_new_document(index_name="data_catalog", document_id=self.asset_id, document=new_asset)

        return new_asset
    
    def update(self):
        '''
        Adds an asset to Elasticsearch
        '''
        es.update_document(document_id=self.asset_id, document=self.model_dump())

    def add_child(self, child):
        '''
        Adds a child to the asset
        '''
        self.children.append(child)
        self.update()

    def delete(self):
        '''
        Deletes an asset from Elasticsearch
        '''
        es.delete_document(document_id=self.asset_id)

    @staticmethod
    def find_one(asset_id: str, asset_type: str = None):
        '''
        Searches for an asset by id and returns it
        '''
        query = {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "_id": asset_id
                            }
                        }
                    ]
                }
            }
        if asset_type is not None:
            query["bool"]["must"].append(
                {
                    "match": {
                        "asset_type": asset_type
                    }
                }
            )
        results = es.search(index_name="data_catalog", query=query)
        
        if len(results) == 0:
            return None
        else:
            return results[0]
