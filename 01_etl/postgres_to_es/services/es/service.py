import json
import logging
import os

from config.settings import es_settings
from elasticsearch import Elasticsearch, helpers
from services.etl import backoff

logger = logging.getLogger(__name__)
es_log = logging.getLogger('elasticsearch')
es_log.setLevel(logging.CRITICAL)


class ElasticsearchService:
    def __init__(self, settings=es_settings):
        self.host = settings.ES_HOST
        self.port = settings.ES_PORT
        self.index_name = settings.ES_INDEX
        self.client = Elasticsearch([{'host': self.host, 'port': self.port}])
        self.status = True if self.__get_status_connect() else False
        self.schema = self.__get_schema(file_path=settings.ES_SCHEMA)
        self.indexes = self.get_indexes()

    @backoff(exception=ConnectionError)
    def __get_status_connect(self):
        logger.info('Connecting to Elasticsearch ...')
        if not self.client.ping():
            raise ConnectionError('No connection to Elasticsearch')
        logger.info('Connected to Elasticsearch completed')
        return True

    @staticmethod
    def __get_schema(file_path: str):
        if os.path.exists(file_path):
            with open(file_path) as json_file:
                return json.load(json_file)
        else:
            logger.warning("Index schema file '%s' not found", file_path)
            return None

    @backoff(exception=ConnectionError)
    def get_indexes(self) -> list:
        logger.info('Get indexes')
        indexes = list(self.client.indices.get_alias().keys())
        if self.index_name not in indexes:
            self.create_index(self.index_name)
        return indexes

    def close(self):
        self.client.transport.close()
        logger.info('Elasticsearch connection closed')

    def create_index(self, index_name: str):
        if body := self.schema:
            self.client.indices.create(index=index_name, body=body)
            logger.info(f"Index '{index_name}' created")
        else:
            logger.warning(
                "Index '%s' not created, missing schema", index_name)

    @backoff(exception=ConnectionError)
    def transfer_data(self, actions) -> None:
        """Добавляет пакеты данных в Elasticsearch"""
        success, failed = helpers.bulk(
            client=self.client,
            actions=[
                {'_index': self.index_name, '_id': action.get('id'), **action}
                for action in actions
            ],
            stats_only=True
        )
        logger.info('Transfer data: success: %s, failed: %s', success, failed)
