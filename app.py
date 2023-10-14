'''
Main application entry point
'''

from services.Logger import get_logger
import uvicorn

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse

from routes.Ingest import bp as ingest_api
from routes.Search import bp as search_api
from routes.Domain import bp as domain_api
from routes.Database import bp as database_api
from routes.Table import bp as table_api
from routes.Column import bp as column_api

from models.Domain import Domain

# Create a FastAPI app
app = FastAPI()

logger = get_logger(__name__)

# Mount the API endpoints
app.mount("/ingest/", ingest_api)
app.mount("/search/", search_api)
app.mount("/domains/", domain_api)
app.mount("/databases/", database_api)
app.mount("/tables/", table_api)
app.mount("/columns/", column_api)

templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
async def home_visualization(request: Request):
    '''
    '''
    domains = Domain.get_all()
    return templates.TemplateResponse("index.html", {"request": request, "domains": domains})



if __name__ == "__main__":
    # uvicorn app:app --host 0.0.0.0 --port 8000 --reload
    uvicorn.run("__main__:app", host="0.0.0.0", port=8000, reload=True, workers=1)
