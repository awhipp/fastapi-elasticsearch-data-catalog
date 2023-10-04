import uvicorn
from fastapi import FastAPI, HTTPException, Query
from models.Asset import Asset
from models.Domain import Domain
from models.Database import Database
from models.Table import Table
from models.Column import Column

# Create a FastAPI app
app = FastAPI()


from services.Logger import get_logger
logger = get_logger(__name__)

# Domains
# TODO breakout into its own controller 
@app.post("/domains", response_model=Domain)
async def create_domain(domain: Domain):
    '''
    Creates a domain and returns it
    '''
    logger.info(f"Creating domain with name: {domain.name}")
    return domain.create()

@app.get("/domains", response_model=Domain)
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

@app.post("/databases", response_model=Database)
async def create_databases(database: Database):
    '''
    Creates a domain and returns it
    '''
    logger.info(f"Creating database with name: {database.name} and domain_id: {database.parent_id}")
    return database.create(parent_id=database.parent_id)

@app.get("/databases", response_model=Database)
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

@app.post("/tables", response_model=Table)
async def create_table(table: Table):
    '''
    Creates a table and returns it
    '''
    logger.info(f"Creating table with name: {table.name} and database id: {table.parent_id}")
    return table.create(parent_id=table.parent_id)

@app.get("/tables", response_model=Table)
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

@app.post("/columns", response_model=Column)
async def create_column(column: Column):
    '''
    Creates a column and returns it
    '''
    logger.info(f"Creating column with name: {column.name} and table id: {column.parent_id}")
    return column.create(parent_id=column.parent_id, d_type=column.data_type)

@app.get("/columns", response_model=Column)
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

if __name__ == "__main__":
    # uvicorn app:app --host 0.0.0.0 --port 8000 --reload
    uvicorn.run("__main__:app", host="0.0.0.0", port=8000, reload=True, workers=1)
