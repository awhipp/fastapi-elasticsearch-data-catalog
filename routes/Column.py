'''
Database Routes
'''

from models.Asset import Asset
from models.Column import Column

from services.Logger import get_logger
logger = get_logger(__name__)

from fastapi import FastAPI, HTTPException, Query
bp = FastAPI()

# /columns routes

@bp.post("/", response_model=Column)
async def create_column(column: Column):
    '''
    Creates a column and returns it
    '''
    logger.info(f"Creating column with name: {column.name} and table id: {column.parent_id}")
    return column.create(parent_id=column.parent_id, d_type=column.data_type)

@bp.get("/", response_model=Column)
async def search_column(
    asset_id: str = Query(default="", description="Search id for column name")
):
    '''
    Searches for a column by name and returns it
    '''
    if asset_id != "" or asset_id is not None:
        logger.info(f"Searching for column with table_id: {asset_id}")
        result = Asset.find_one(asset_id=asset_id, asset_type="column")
        if result is None:
            raise HTTPException(status_code=404, detail=f"Column with column_id: {asset_id} not found")
    else:
        raise HTTPException(status_code=400, detail="Column ID cannot be None")
    
    return result