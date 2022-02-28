from typing import Optional

from dotenv import load_dotenv
from pydantic import BaseSettings

load_dotenv()


class PostgresSettings(BaseSettings):
    """Параметры настроек для PostgreSQL"""
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str
    DB_HOST: Optional[str] = 'localhost'
    DB_PORT: Optional[int] = 5432
    DB_OPTIONS: str

    class Config:
        env_file = '.env'


class ElasticsearchSettings(BaseSettings):
    """Параметры настроек для Elasticsearch"""
    ES_HOST: str = 'localhost'
    ES_PORT: Optional[int] = 9200
    ES_INDEX: str
    ES_SCHEMA: str

    class Config:
        env_file = '.env'


class ETLSettings(BaseSettings):
    """Параметры настроек для ETL"""
    LIMIT: Optional[int] = 100
    UPLOAD_INTERVAL: float
    STATE_FIELD: str
    STATE_FILE_NAME: str

    class Config:
        env_file = '.env'


pg_settings = PostgresSettings()
es_settings = ElasticsearchSettings()
etl_settings = ETLSettings()
