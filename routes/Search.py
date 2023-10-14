'''
Search Routes
'''
from services.ElasticsearchService import ElasticsearchService

# Initialize an Elasticsearch client
es = ElasticsearchService(hosts=["http://localhost:9200"])

from services.Logger import get_logger
logger = get_logger(__name__)

from fastapi import FastAPI, Query
bp = FastAPI()

# /ingest routes

@bp.get("/names")
async def search_names(value: str = Query(default="", description="Search partial for all assets")):
    '''
    Search all assets based on partial names
    '''
    logger.info(f"Searching for all assets with name that contains: {value}")
    return es.search(
        index_name="data_catalog",
        query={
                "wildcard": {
                    "name": f"*{value}*"
                }
            }
        )
