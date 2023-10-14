from services.ElasticsearchService import ElasticsearchService
from models.Asset import Asset
from fastapi import HTTPException
from uuid import uuid4

from services.Logger import get_logger
logger = get_logger(__name__)


# Initialize an Elasticsearch client
es = ElasticsearchService(hosts=["http://localhost:9200"])

# Create a Pydantic model for the domain
class Domain(Asset):
    '''
    Pydantic model for a domain
    '''
    asset_type: str = "domain"
    parent_id: str = ""

    def create(self, parent_id=None):
        '''
        Creates a domain and returns it
        '''
        self.asset_id = str(uuid4())
        new_domain = self.model_dump()
        
        if parent_id is not None:
            raise HTTPException(status_code=400, detail=f"Domains cannot have parents")

        domain_check = self.find_one(asset_id=self.asset_id, asset_type="domain")

        if domain_check is not None:
            raise HTTPException(status_code=400, detail=f"Domain with id: {self.asset_id} already exists")
            
        es.insert_new_document(index_name="data_catalog", document_id=self.asset_id, document=new_domain)
        return new_domain
    
    @staticmethod
    def get_all():
        '''
        Returns all assets with type domain
        '''
        return Asset.find_all(asset_type="domain")