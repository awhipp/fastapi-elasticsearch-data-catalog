from elasticsearch import Elasticsearch
import time
from elasticsearch.exceptions import ConnectionError as ElasticsearchConnectionError
from fastapi import HTTPException

import os
from services.Logger import get_logger
logger = get_logger(__name__)


class ElasticsearchService:
    '''Singleton class for Elasticsearch client'''
    _instance = None
    client = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.connect(**kwargs)
        return cls._instance

    def wait_for_elastic_search(self, timeout=5):
        """
        Wait for ElasticSearch to start up
        """
        logger.info("Waiting for Elasticsearch to start up")

        attempts = 1

        while attempts < 12:
            try:
                self.client.cluster.health(
                    wait_for_status='yellow',
                    timeout=f'{timeout}s'
                )
                logger.info("Elasticsearch is up")
                break
            except ElasticsearchConnectionError:
                logger.info("Elasticsearch is down")
                attempts += 1
                time.sleep(timeout)


    def connect(self, **kwargs):
        '''Connects to Elasticsearch client'''
        hosts = kwargs.get('hosts', ["http://localhost:9200"])
        self.client = Elasticsearch(hosts=hosts)

        wait_for_elastic = os.environ.get('WAIT_FOR_ELASTICSEARCH', 'false')
        wait_for_elastic = wait_for_elastic.lower() == 'true'
        if wait_for_elastic:
            self.wait_for_elastic_search()

        logger.info(f"Elasticsearch client connected to hosts: {hosts}")
        self.create_index_if_not_exists()

    def create_index_if_not_exists(self, index_name="data_catalog"):
        '''Creates an index if it doesn't exist'''
        if not self.client.indices.exists(index=index_name):
            self.client.indices.create(index=index_name)
            logger.info(f"Created index: {index_name}")
        else:
            logger.info(f"Index already exists: {index_name}")

    def delete_index(self, index_name="data_catalog"):
        '''Deletes an index'''
        if self.client.indices.exists(index=index_name):
            self.client.indices.delete(index=index_name)
            logger.info(f"Deleted index: {index_name}")
        else:
            logger.info(f"Index does not exist: {index_name}")

    def search(self, index_name="data_catalog", query=None):
        '''Searches for a query in an index'''
        if query is None:
            raise HTTPException(status_code=400, detail="Query cannot be None")

        try:
            response = self.client.search(
                index=index_name,
                query=query
            )
            results = [hit["_source"] for hit in response["hits"]["hits"]]
            return results
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error executing Elasticsearch query: {str(e)}")


    def insert_new_document(self, index_name="data_catalog", document_id=None, document=None):
        '''Inserts a new document into an index'''
        if document is None:
            raise HTTPException(status_code=400, detail="Document cannot be None")
        if document_id is None:
            raise HTTPException(status_code=400, detail="Document ID cannot be None")

        try:
            response = self.client.index(
                index=index_name,
                id=document_id,
                document=document,
                refresh='wait_for'
            )
            return response
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error inserting new document into Elasticsearch index: {str(e)}")
        
    def update_document(self, index_name="data_catalog", document_id=None, document=None):
        '''Updates a document in an index'''
        if document_id is None:
            raise HTTPException(status_code=400, detail="Document ID cannot be None")
        if document is None:
            raise HTTPException(status_code=400, detail="Document cannot be None")

        try:
            response = self.client.update(
                index=index_name,
                id=document_id,
                doc=document,
                refresh='wait_for'
            )
            logger.info(f"Updated document with id: {document_id}")
            logger.info(response)
            return response
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error updating document in Elasticsearch index: {str(e)}")
        
    def delete_document(self, index_name="data_catalog", document_id=None):
        '''Deletes a document in an index'''
        if document_id is None:
            raise HTTPException(status_code=400, detail="Document ID cannot be None")

        try:
            response = self.client.delete(
                index=index_name,
                id=document_id,
                refresh='wait_for'
            )
            logger.info(f"Deleted document with id: {document_id}")
            logger.info(response)
            return response
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error deleting document in Elasticsearch index: {str(e)}")

# Usage example:
# es = ElasticsearchSingleton(hosts=["http://your-elasticsearch-host:9200"])
# results = es.search("your_index_name", "your_query")
# print(results)