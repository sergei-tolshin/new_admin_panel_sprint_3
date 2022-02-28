from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


class UUIDMixin(BaseModel):
    id: str


class ModifiedMixin(BaseModel):
    modified: datetime


class PGBaseModel(UUIDMixin, ModifiedMixin):
    """Базовая модель"""


class FilmworkModel(PGBaseModel):
    """Модель фильмов"""


class GenreModel(PGBaseModel):
    """Модель жанров"""


class PersonModel(PGBaseModel):
    """Модель персонажей"""


class GenreFilmworkModel(PGBaseModel):
    """Модель фильмов по жанрам"""


class PersonFilmworkModel(PGBaseModel):
    """Модель фильмов где участвуют персонажи"""


class ESPersonModel(UUIDMixin):
    """Модель экземпляров персонажей для ElasticSearch"""
    name: Optional[str]


class ESFilmworkModel(UUIDMixin):
    """Модель экземпляров фильмов для ElasticSearch"""
    imdb_rating: Optional[float] = None
    genre: Optional[List[str]] = None
    title: str
    description: Optional[str] = None
    director: Optional[List[str]] = None
    actors_names: Optional[List[str]] = None
    writers_names: Optional[List[str]] = None
    actors: Optional[List[ESPersonModel]] = None
    writers: Optional[List[ESPersonModel]] = None
