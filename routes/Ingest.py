'''
Database Routes
'''

from models.Domain import Domain
from models.Database import Database
from models.Table import Table
from models.Column import Column

from services.Logger import get_logger
logger = get_logger(__name__)

from fastapi import FastAPI
bp = FastAPI()

# /ingest routes

@bp.post("/full_source")
async def full_source(full_catalog: dict):
    '''
    Ingests the full catalog
    '''
    response = []
    for domain in full_catalog:
        domain_asset = Domain(name=domain)
        response.append(domain_asset.create())
        for database in full_catalog[domain]:
            database_asset = Database(name=database, parent_id=domain_asset.asset_id)
            response.append(database_asset.create())
            for table in full_catalog[domain][database]:
                table_asset = Table(name=table, parent_id=database_asset.asset_id)
                response.append(table_asset.create())
                for column in full_catalog[domain][database][table]:
                    column_asset = Column(
                        name=column, 
                        parent_id=table_asset.asset_id, 
                        data_type=full_catalog[domain][database][table][column]
                    )
                    response.append(column_asset.create())

    return response

