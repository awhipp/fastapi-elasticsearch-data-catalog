'''
Domain Routes
'''

from models.Asset import Asset
from models.Domain import Domain
from models.Database import Database

from services.Logger import get_logger
logger = get_logger(__name__)

from fastapi import FastAPI, HTTPException, Query
bp = FastAPI()

# /domains routes

@bp.post("/", response_model=Domain)
async def create_domain(domain: Domain):
    '''
    Creates a domain and returns it
    '''
    logger.info(f"Creating domain with name: {domain.name}")
    return domain.create()

@bp.get("/", response_model=Domain)
async def search_domains(
    asset_id: str = Query(default="", description="Search id for domains")
):
    '''
    Searches for a domain by name and returns it
    '''

    if asset_id != "" or asset_id is not None:
        logger.info(f"Searching for domain with id: {asset_id}")
        result = Asset.find_one(asset_id=asset_id, asset_type="domain")
    else:
        raise HTTPException(status_code=400, detail="Domain ID cannot be None")
    
    if result is None:
        raise HTTPException(status_code=404, detail=f"Domain with id: {asset_id} not found")
    
    return result

@bp.get("/children", response_model=list[Database])
async def get_children(
    parent_asset_id: str = Query(default="", description="Search id for parent domain")
):
    '''
    Searches for a domain by name and returns it
    '''

    if parent_asset_id != "" or parent_asset_id is not None:
        logger.info(f"Searching for children of domain with id: {parent_asset_id}")
        results = Asset.find_children(asset_id=parent_asset_id)
    else:
        raise HTTPException(status_code=400, detail="Parent Domain ID cannot be None")
    
    return results