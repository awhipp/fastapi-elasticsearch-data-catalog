'''
Ingest Routes
'''
from uuid import uuid4
import random

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


@bp.post("/sample_data", response_class=list)
async def sample_data(number_domains: int = 1, number_databases: int = 2, number_tables: int = 3, number_columns: int = 5):
    '''
    Ingests sample data
    '''
    suffix = uuid4().hex[:6]
    response = []
    for domain in range(number_domains):
        domain_asset = Domain(name=f"domain_{suffix}_{domain}")
        response.append(domain_asset.create())
        for database in range(number_databases):
            database_asset = Database(name=f"database_{suffix}_{database}", parent_id=domain_asset.asset_id)
            response.append(database_asset.create())
            for table in range(number_tables):
                table_asset = Table(name=f"table_{suffix}_{table}", parent_id=database_asset.asset_id)
                response.append(table_asset.create())
                for column in range(number_columns):
                    column_asset = Column(
                        name=f"column_{suffix}_{column}", 
                        parent_id=table_asset.asset_id, 
                        data_type=random.choice(["string", "integer", "float", "boolean", "date"])
                    )
                    response.append(column_asset.create())

    return response