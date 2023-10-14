'''
Database Routes
'''

from models.Asset import Asset
from models.Database import Database

from services.Logger import get_logger
logger = get_logger(__name__)

from fastapi import FastAPI, HTTPException, Query
bp = FastAPI()

# /databases routes

@bp.post("/", response_model=Database)
async def create_databases(database: Database):
    '''
    Creates a domain and returns it
    '''
    logger.info(f"Creating database with name: {database.name} and domain_id: {database.parent_id}")
    return database.create(parent_id=database.parent_id)

@bp.get("/", response_model=Database)
async def search_databases(
    asset_id: str = Query(default="", description="Search id for database name")
):
    '''
    Searches for a database by name and returns it
    '''
    if asset_id != "" or asset_id is not None:
        logger.info(f"Searching for database with database_id: {asset_id}")
        result = Asset.find_one(asset_id=asset_id, asset_type="database")
        if result is None:
            raise HTTPException(status_code=404, detail=f"Database with database_id: {asset_id} not found")
    else:
        raise HTTPException(status_code=400, detail="Database ID cannot be None")
    
    return result
