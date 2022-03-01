import abc
import json
import os
from pathlib import Path
from typing import Any, Optional, Union

from config.settings import etl_settings

state_file_name: str = etl_settings.STATE_FILE_NAME
default_file_path: str = f'{Path(__file__).resolve().parent}'


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None,
                 file_name: Optional[str] = None):
        self.file_path = file_path
        self.file_name = file_name
        self.file_state = f'{self.file_path}{state_file_name}'

    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        file_state = self.retrieve_state() or {}
        with open(self.file_state, 'w') as storage:
            save_state: dict = {**file_state, **state}
            json.dump(save_state, storage, ensure_ascii=False, indent=4)

    def retrieve_state(self) -> Union[dict, None]:
        """Загрузить состояние локально из постоянного хранилища"""
        if os.path.exists(self.file_state):
            with open(self.file_state, 'r') as storage:
                return json.load(storage)
        else:
            return None


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не
    перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или
    распределённым хранилищем.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage
        self.state = self.storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        self.storage.save_state(state={key: value})

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        if self.state is not None:
            return self.state.get(key)
        return None
