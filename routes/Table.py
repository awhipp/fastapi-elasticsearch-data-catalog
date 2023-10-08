'''
Database Routes
'''

from models.Asset import Asset
from models.Table import Table

from services.Logger import get_logger
logger = get_logger(__name__)

from fastapi import FastAPI, HTTPException, Query
bp = FastAPI()

# /tables routes

@bp.post("/", response_model=Table)
async def create_table(table: Table):
    '''
    Creates a table and returns it
    '''
    logger.info(f"Creating table with name: {table.name} and database id: {table.parent_id}")
    return table.create(parent_id=table.parent_id)

@bp.get("/", response_model=Table)
async def search_tables(
    asset_id: str = Query(default="", description="Search id for table name")
):
    '''
    Searches for a table by name and returns it
    '''
    if asset_id != "" or asset_id is not None:
        logger.info(f"Searching for table with table_id: {asset_id}")
        result = Asset.find_one(asset_id=asset_id, asset_type="table")
        if result is None:
            raise HTTPException(status_code=404, detail=f"Table with table_id: {asset_id} not found")
    else:
        raise HTTPException(status_code=400, detail="Table ID cannot be None")
    
    return result