from dataclasses import dataclass
import uuid
import datetime


@dataclass
class Filmwork:
    title: str
    description: str
    rating: float
    id: uuid.UUID
    creation_date: datetime.date
    type: str
    created: datetime.date
    modified: datetime.date


@dataclass
class Person:
    full_name: str
    id: uuid.UUID
    created: datetime.date
    modified: datetime.date


@dataclass
class Genre:
    name: str
    description: str
    id: uuid.UUID
    created: datetime.date
    modified: datetime.date


@dataclass
class PersonFilmwork:
    role: str
    id: uuid.UUID
    film_work_id: uuid.UUID
    person_id: uuid.UUID
    created: datetime.date


@dataclass
class GenreFilmwork:
    id: uuid.UUID
    film_work_id: uuid.UUID
    genre_id: uuid.UUID
    created: datetime.date
