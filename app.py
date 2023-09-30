import uvicorn
from fastapi import FastAPI, HTTPException, Query
from models.Domain import Domain
from models.Database import Database
from utilities.DomainHelpers import search as domain_search
from utilities.DatabaseHelpers import search as database_search

# Create a FastAPI app
app = FastAPI()


from services.Logger import get_logger
logger = get_logger(__name__)

# Domains
# TODO breakout into its own controller 
@app.post("/domains/", response_model=Domain)
async def create_domain(domain: Domain):
    '''
    Creates a domain and returns it
    '''
    logger.info(f"Creating domain with name: {domain.name}")
    return domain.create_domain()

@app.get("/domains/", response_model=Domain)
async def search_domains(
    name: str = Query(default="", description="Search name for domains"),
    domain_id: str = Query(default="", description="Search id for domains")
):
    '''
    Searches for a domain by name and returns it
    '''

    if domain_id != "" and name != "":
        raise HTTPException(status_code=400, detail="Name or id must be provided")
    elif domain_id != "":
        logger.info(f"Searching for domain with id: {domain_id}")
        query = domain_id
        results = domain_search(domain_id=query)
    else:
        logger.info(f"Searching for domain with name: {name}")
        query = name
        results = domain_search(name=query)
    if len(results) == 0:
        raise HTTPException(status_code=404, detail=f"Domain with name/id: {query} not found")
    elif len(results) > 1:
        raise HTTPException(status_code=500, detail=f"Found multiple domains with name/id: {query}")
    return results[0]

@app.post("/databases/", response_model=Database)
async def create_databases(database: Database):
    '''
    Creates a domain and returns it
    '''
    logger.info(f"Creating database with name: {database.name} and domain_id: {database.domain_id}")
    return database.create_database()

@app.get("/databases/", response_model=Database)
async def search_databases(
    domain_id: str = Query(..., description="Domain ID for the database"),
    database_id: str = Query(default="", description="Search id for database name"),
    name: str = Query(default="", description="Search name for database name")
):
    '''
    Searches for a database by name and returns it
    '''
    if database_id != "" and name != "":
        raise HTTPException(status_code=400, detail="Name or id must be provided")
    elif database_id != "":
        logger.info(f"Searching for database with database_id: {database_id}")
        results = database_search(database_id=database_id)
        if len(results) == 0:
            raise HTTPException(status_code=404, detail=f"Database with database_id: {database_id} not found")
    else:
        logger.info(f"Searching for database with name: {name}")
        results = database_search(name=name)
        if len(results) == 0:
            raise HTTPException(status_code=404, detail=f"Database with name: {name} not found")
    
    return results

if __name__ == "__main__":
    # uvicorn app:app --host 0.0.0.0 --port 8000 --reload
    uvicorn.run("__main__:app", host="0.0.0.0", port=8000, reload=True, workers=1)
