import logging
from typing import List, Optional

import psycopg2
from config.settings import etl_settings, pg_settings
from psycopg2 import OperationalError
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from services.etl import backoff
from services.pg import models, queries

logger = logging.getLogger(__name__)


class PostgresService:
    def __init__(self):
        self.state_field = etl_settings.STATE_FIELD
        self.conn = self.__connect()

    @backoff(exception=OperationalError)
    def __connect(self):
        logger.info('Connecting to PostgreSQL ...')
        with PostgresConnector().connection as conn:
            logger.info('Connected to PostgreSQL completed')
            return conn

    def close(self):
        if self.conn:
            self.conn.close()
            logger.info('PostgreSQL connection closed')

    def executor(self, query):
        """Возвращает результат запроса к PostgreSQL"""
        with self.conn.cursor() as curs:
            curs.execute(query)
            return curs.fetchall()

    def get_modified_person(self, timestamp) -> list:
        """Возвращает список новых/измененных персонажей"""
        persons = self.executor(
            query=queries.modified_person(timestamp=timestamp)
        )
        if persons:
            return [models.PersonModel(**person) for person in persons]
        return []

    def get_filmwork_by_person(self, persons: List[str]) -> List[str]:
        """Возвращает список фильмов, в которых участвуют персонажи"""
        filmworks = self.executor(
            query=queries.filmwork_by_person(persons=persons)
        )
        return [
            models.PersonFilmworkModel(**filmwork).id for filmwork in filmworks
        ]

    def get_modified_genre(self, timestamp) -> list:
        """Возвращает список новых/измененных жанров"""
        genres = self.executor(
            query=queries.modified_genre(timestamp=timestamp)
        )
        if genres:
            return [models.GenreModel(**genre) for genre in genres]
        return []

    def get_filmwork_by_genre(self, genres: List[str]) -> List[str]:
        """Возвращает список фильмов по жанрам"""
        filmworks = self.executor(
            query=queries.filmwork_by_genre(genres=genres)
        )
        return [
            models.GenreFilmworkModel(**filmwork).id for filmwork in filmworks
        ]

    def get_modified_filmwork(self, timestamp) -> list:
        """Возвращает список новых/измененных фильмов"""
        filmworks = self.executor(
            query=queries.modified_filmworks(timestamp=timestamp)
        )
        if filmworks:
            return [models.FilmworkModel(**filmwork) for filmwork in filmworks]
        return []

    def get_filmwork_instances(self, ids: tuple) -> None:
        """Возвращает экземпляры фильмов"""
        if ids:
            return self.executor(query=queries.filmwork_by_id(ids=ids))
        return None


class PostgresConnector:
    def __init__(self, settings=pg_settings):
        self.dsl: dict = {
            'dbname': settings.DB_NAME,
            'user': settings.DB_USER,
            'password': settings.DB_PASSWORD,
            'host': settings.DB_HOST,
            'port': settings.DB_PORT,
            'options': settings.DB_OPTIONS,
        }
        self.conn: Optional[_connection] = None

    def __create_conn(self) -> _connection:
        return psycopg2.connect(**self.dsl, cursor_factory=DictCursor)

    @property
    def connection(self):
        if self.conn and not self.conn.closed:
            return self.conn
        else:
            return self.__create_conn()
