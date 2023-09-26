from services.ElasticsearchService import ElasticsearchService
from fastapi import HTTPException

from pydantic import UUID4

from services.Logger import get_logger
logger = get_logger(__name__)

def search(name: str=None, id: UUID4=None):
    '''
    Searches for a domain by name and returns it
    '''
    # Initialize an Elasticsearch client
    es = ElasticsearchService(hosts=["http://localhost:9200"])

    if name is not None:
        query = {
            "match": {
                "name": name
            }
        }
    elif id is not None:
        query = {
            "match": {
                "id": str(id)
            }
        }
    else:
        raise HTTPException(status_code=400, detail="Name or id must be provided")
    
    results = es.search(index_name="data_catalog", query=query)
    logger.info(f"Found {len(results)} domains with id/name: {str(name) or str(id)}")
    return results
