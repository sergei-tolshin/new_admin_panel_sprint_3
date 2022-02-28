import logging
from typing import Optional

from config.settings import etl_settings
from services.es import ElasticsearchService
from services.pg import PostgresService, models

from .state import etl_state

logger = logging.getLogger(__name__)


class ETL:
    def __init__(self, settings=etl_settings):
        self.conf = settings
        self.pg_client = PostgresService()
        self.es_client = ElasticsearchService()
        self.state = etl_state.get_state('modified') or {}

    def extract(self) -> None:
        """
        Извлекает из PostgreSQL все новые/измененные
        данные (жанры, персонажи и фильмы).
        Возвращает количество уникальных ID фильмов и экземпляры фильмов
        """
        person_filmwork_ids: list[str] = []
        genre_filmwork_ids: list[str] = []

        """
        Получаем список ID новых/измененныех персонажей и список ID фильмов
        свызанных, в которых участвуют выбранные персонажи
        """
        if persons := self.pg_client.get_modified_person(state=self.state):
            person_filmwork_ids = self.pg_client.get_filmwork_by_person(
                persons=persons)

        """
        Получаем список ID новых/измененных жанров и список ID фильмов
        по выбранным жанрам
        """
        if genres := self.pg_client.get_modified_genre(state=self.state):
            genre_filmwork_ids = self.pg_client.get_filmwork_by_genre(
                genres=genres)

        """
        Получаем список ID новых/измененных фильмов
        """
        filmwork_ids = self.pg_client.get_modified_filmwork(state=self.state)

        """Формируем множество уникальных ID новых/измененных фильмов"""
        unique_filmwork_ids = set(
            filmwork_ids + person_filmwork_ids + genre_filmwork_ids)

        """
        Возвращаем все новые/измененные экземпляры фильмов
        """
        filmwork_instances = self.pg_client.get_filmwork_instances(
            ids=tuple(unique_filmwork_ids))
        return len(unique_filmwork_ids), filmwork_instances

    def transform(self, modified_data) -> None:
        """
        Трансформирует извлеченные экземпляры фильмов для Elasticsearch.
        Формирует список уникальных фильмов с группировкой списков и
        экземпляров genre, director, actor, writer
        """
        if modified_data is not None:
            transformed_data: list = []
            filmwork_ids: set = {filmwork.get('fw_id')
                                 for filmwork in modified_data}
            for filmwork_id in filmwork_ids:
                genres: Optional[list[str]] = []
                directors: Optional[list[str]] = []
                actors_names: Optional[list[str]] = []
                writers_names: Optional[list[str]] = []
                actors: Optional[list[dict[str, str]]] = []
                writers: Optional[list[dict[str, str]]] = []
                for filmwork in modified_data:
                    if filmwork.get('fw_id') == filmwork_id:
                        imdb_rating = filmwork.get('rating')
                        title = filmwork.get('title')
                        description = filmwork.get('description')
                        if filmwork.get('genre') not in genres:
                            genres.append(filmwork.get('genre'))
                        person_name = filmwork.get('full_name')
                        person_instance = {'id': filmwork.get('person_id'),
                                           'name': person_name}
                        if filmwork.get('role') == 'director':
                            if person_name not in directors:
                                directors.append(person_name)
                        elif filmwork.get('role') == 'actor':
                            if person_name not in actors_names:
                                actors_names.append(person_name)
                            if person_instance not in actors:
                                actors.append(person_instance)
                        elif filmwork.get('role') == 'writer':
                            if person_name not in writers_names:
                                writers_names.append(person_name)
                            if person_instance not in writers:
                                writers.append(person_instance)
                        new_filmwork = {
                            'id': filmwork_id,
                            'imdb_rating': imdb_rating,
                            'title': title,
                            'description': description,
                            'genre': genres,
                            'director': directors,
                            'actors_names': actors_names,
                            'writers_names': writers_names,
                            'actors': actors,
                            'writers': writers,
                        }
                transformed_data.append(new_filmwork)
            for filmwork in transformed_data:
                yield models.ESFilmworkModel(**filmwork).dict()

    def load(self, transformed_data):
        """
        Формируем пакеты фильмов и загружаем их в Elasticsearch
        """
        actions: list = []
        for data in transformed_data:
            actions.append(data)
            if len(actions) == self.conf.LIMIT:
                self.es_client.transfer_data(actions=actions)
                actions.clear()
        else:
            if actions:
                self.es_client.transfer_data(actions=actions)
                pass

    def save_state(self):
        """Сохраняем последнее состояние"""
        etl_state.set_state('modified', self.state)

    def close_connect(self):
        """Закрываем подключения"""
        self.es_client.close()
        self.pg_client.close()
