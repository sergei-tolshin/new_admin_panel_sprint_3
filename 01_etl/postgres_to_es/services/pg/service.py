import logging
from datetime import datetime
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
        self.pg = self.__cursor(self.conn)

    @backoff(exception=OperationalError)
    def __connect(self):
        logger.info('Connecting to PostgreSQL ...')
        with PostgresConnector().connection as pg_conn:
            logger.info('Connected to PostgreSQL completed')
            return pg_conn

    def __cursor(self, connect):
        return PostgresCursor(conn=connect).cursor

    def close(self):
        if self.conn:
            self.pg.close()
            self.conn.close()
            logger.info('PostgreSQL connection closed')

    def executor(self, query):
        """Возвращает результат запроса к PostgreSQL"""
        self.pg.execute(query)
        return self.pg.fetchall()

    def get_modified_person(self, state) -> list:
        """Возвращает список новых/измененных персонажей"""
        modified: str = state.get('person') or datetime.min
        persons = self.executor(
            query=queries.modified_person(timestamp=modified)
        )
        if persons:
            state['person'] = f'{persons[-1].get(self.state_field)}'
            return [models.PersonModel(**person).id for person in persons]
        return []

    def get_filmwork_by_person(self, persons: List[str]) -> List[str]:
        """Возвращает список фильмов, в которых участвуют персонажи"""
        filmworks = self.executor(
            query=queries.filmwork_by_person(persons=persons)
        )
        return [
            models.PersonFilmworkModel(**filmwork).id for filmwork in filmworks
        ]

    def get_modified_genre(self, state) -> list:
        """Возвращает список новых/измененных жанров"""
        modified: str = state.get('genre') or datetime.min
        genres = self.executor(
            query=queries.modified_genre(timestamp=modified)
        )
        if genres:
            state['genre'] = f'{genres[-1].get(self.state_field)}'
            return [models.GenreModel(**genre).id for genre in genres]
        return []

    def get_filmwork_by_genre(self, genres: List[str]) -> List[str]:
        """Возвращает список фильмов по жанрам"""
        filmworks = self.executor(
            query=queries.filmwork_by_genre(genres=genres)
        )
        return [
            models.GenreFilmworkModel(**filmwork).id for filmwork in filmworks
        ]

    def get_modified_filmwork(self, state) -> list:
        """Возвращает список новых/измененных фильмов"""
        modified: str = state.get('filmwork') or datetime.min
        filmworks = self.executor(
            query=queries.modified_filmworks(timestamp=modified)
        )
        if filmworks:
            state['filmwork'] = f'{filmworks[-1].get(self.state_field)}'
            return [models.FilmworkModel(**filmwork).id
                    for filmwork in filmworks]
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


class PostgresCursor:
    def __init__(self, conn):
        self._cursor: Optional[DictCursor] = None
        self._conn = conn

    def __create_cursor(self) -> DictCursor:
        return self._conn.cursor()

    @property
    def cursor(self) -> DictCursor:
        if self._cursor and not self._cursor.closed:
            return self._cursor
        else:
            return self.__create_cursor()
