from uuid import UUID
from typing import Any, List
from datetime import datetime

from pydantic import BaseModel

from .base_storage import BaseStorage


class State:
    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        try:
            state = self.storage.retrieve_state()
        except FileNotFoundError:
            state = dict()
        state[key] = str(value)
        self.storage.save_state(state)

    def get_state(self, key: str) -> Any:
        return self.storage.retrieve_state().get(key)


class Person(BaseModel):
    uuid: UUID
    full_name: str

class Genre(BaseModel):
    uuid: UUID
    name: str

class FilmWork(BaseModel):
    uuid: UUID
    imdb_rating: float
    title: str
    description: str
    genre: List[Genre]
    actors: List[Person] = []
    writers: List[Person] = []
    directors: List[Person] = []
