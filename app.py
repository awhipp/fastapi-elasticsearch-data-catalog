import uvicorn
from fastapi import FastAPI, HTTPException, Query
from models.Asset import Asset
from models.Domain import Domain
from models.Database import Database
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
        results = Asset.search(asset_id=asset_id)
    else:
        raise HTTPException(status_code=400, detail="Domain ID cannot be None")
    
    if len(results) == 0:
        raise HTTPException(status_code=404, detail=f"Domain with id: {asset_id} not found")
    elif len(results) > 1:
        raise HTTPException(status_code=500, detail=f"Found multiple domains wit hid: {asset_id}")
    
    return results[0]

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
        results = Asset.search(asset_id=asset_id)
        if len(results) == 0:
            raise HTTPException(status_code=404, detail=f"Database with database_id: {asset_id} not found")
    else:
        raise HTTPException(status_code=400, detail="Database ID cannot be None")
    
    return results

if __name__ == "__main__":
    # uvicorn app:app --host 0.0.0.0 --port 8000 --reload
    uvicorn.run("__main__:app", host="0.0.0.0", port=8000, reload=True, workers=1)
