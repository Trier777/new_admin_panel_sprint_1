import datetime
import sqlite3
import psycopg2 as psycopg2
from dotenv import load_dotenv
from os import path
import os
import uuid
from dataclasses import dataclass

from psycopg2.extras import DictCursor

load_dotenv()

DB_PATH = os.environ.get('DB_PATH')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
UPLOAD_PORTION = os.environ.get('UPLOAD_PORTION')


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


class SQLiteLoader:

    def __init__(self, connection):
        self.connection = connection

    def load_movies(self):
        tables_dict = {}
        tables_dict["person"] = self.load_table("person")
        tables_dict["filmwork"] = self.load_table("film_work")
        tables_dict["genre"] = self.load_table("genre")
        tables_dict["genre_film_work"] = self.load_table("genre_film_work")
        tables_dict["person_film_work"] = self.load_table("person_film_work")
        return tables_dict

    def load_table(self, tablename) -> list:
        query = "SELECT * FROM {table};".format(table=tablename)
        read_data = self.execute_read_query(self.connection, query)
        if read_data:
            return read_data
        else:
            return []

    @staticmethod
    def execute_read_query(connection: sqlite3.Connection, query: str) -> list or bool:
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as e:
            print("The error {} read occurred".format(e))
            return False


class PostgresSaver:

    def __init__(self, pg_conn):
        self.pg_conn = pg_conn

    def save_row(self, cursor: DictCursor, query_write: str, values: tuple):
        try:
            cursor.execute(query_write, values)
            self.pg_conn.commit()
            print("Success!")
        except Exception as e:
            print("The error {} write occurred".format(e))

    def save_all_data(self, data):
        for k in data.keys():
            self.save_table(k, data[k])


    def save_person_to_postgres(self, person: Person):
        cursor = self.pg_conn.cursor()
        query_write = 'INSERT INTO content.person (id, full_name, created, modified) VALUES (%s, %s, %s, %s)' \
                      'ON CONFLICT (id) DO NOTHING;'
        values = (person.id, person.full_name, person.created, person.modified)
        self.save_row(cursor, query_write, values)

    def save_film_work_to_postgres(self, filmwork: Filmwork):
        cursor = self.pg_conn.cursor()
        query_write = 'INSERT INTO content.film_work (id, title, description, rating, created, modified, ' \
                      'creation_date, type)' \
                      'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)' \
                      'ON CONFLICT (id) DO NOTHING;'
        values = (filmwork.id, filmwork.title, filmwork.description,
                  filmwork.rating, filmwork.created, filmwork.modified,
                  filmwork.creation_date, filmwork.type)
        self.save_row(cursor, query_write, values)

    def save_genre_to_postgres(self, genre: Genre):
        cursor = self.pg_conn.cursor()
        query_write = 'INSERT INTO content.genre (id, name, description, created, modified) ' \
                      'VALUES (%s, %s, %s, %s, %s)' \
                      'ON CONFLICT (id) DO NOTHING;'
        values = (genre.id, genre.name, genre.description, genre.created, genre.modified)
        self.save_row(cursor, query_write, values)

    def save_genre_film_work_to_postgres(self, genre_film_work: GenreFilmwork):
        cursor = self.pg_conn.cursor()
        query_write = 'INSERT INTO content.genre_film_work (id, genre_id, film_work_id, created) ' \
                      'VALUES (%s, %s, %s, %s)' \
                      'ON CONFLICT (id) DO NOTHING;'
        values = (genre_film_work.id, genre_film_work.genre_id,
                  genre_film_work.film_work_id, genre_film_work.created)
        self.save_row(cursor, query_write, values)

    def save_person_film_work_to_postgres(self, person_film_work: PersonFilmwork):
        cursor = self.pg_conn.cursor()
        query_write = 'INSERT INTO content.person_film_work (id, film_work_id, person_id, role, created) ' \
                      'VALUES (%s, %s, %s, %s,  %s)' \
                      'ON CONFLICT (id) DO NOTHING;'
        values = (person_film_work.id, person_film_work.film_work_id,
                  person_film_work.person_id, person_film_work.role, person_film_work.created)
        self.save_row(cursor, query_write, values)

    def save_table(self, name: str, table_list: list):
        if name == "person":
            for row in table_list:
                person = Person(full_name=row[1], id=row[0],
                                created=row[2], modified=row[3])
                self.save_person_to_postgres(person)
        elif name == "film_work":
            for row in table_list:
                film_work = Filmwork(id=row[0], title=row[1], description=row[2], creation_date=row[3],
                                     rating=row[5], type=row[6], created=row[7], modified=row[8])
                self.save_film_work_to_postgres(film_work)
        elif name == "genre":
            for row in table_list:
                genre = Genre(id=row[0], name=row[1], description=row[2], created=row[3], modified=row[4])
                self.save_genre_to_postgres(genre)
        elif name == "genre_film_work":
            for row in table_list:
                genre_film_work = GenreFilmwork(id=row[0], film_work_id=row[1], genre_id=row[2], created=row[3])
                self.save_genre_film_work_to_postgres(genre_film_work)
        elif name == "person_film_work":
            for row in table_list:
                person_film_work = PersonFilmwork(id=row[0], film_work_id=row[1], person_id=row[2],
                                                  role=row[3], created=row[4])
                self.save_person_film_work_to_postgres(person_film_work)


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: psycopg2.extensions.connection):
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_loader = SQLiteLoader(connection)
    data = sqlite_loader.load_movies()
    postgres_saver.save_all_data(data)


def get_script_dir() -> str:
    abs_path = path.abspath(__file__)  # полный путь к файлу скрипта
    return path.dirname(abs_path)


DB_FILE = get_script_dir() + path.sep + DB_PATH


def check_table_row_quantity(table_name: str) -> bool:
    dsl = {'dbname': DB_NAME, 'user': DB_USER, 'password': DB_PASSWORD, 'host': DB_HOST, 'port': DB_PORT}
    with sqlite3.connect(DB_FILE) as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        query_sqlite = "SELECT count(*) FROM {table};".format(table=table_name)
        cursor_sqlite = sqlite_conn.cursor()
        cursor_sqlite.execute(query_sqlite)
        sqlite_quantity_row = cursor_sqlite.fetchone()

        query_pg = "SELECT count(*) FROM content.{table};".format(table=table_name)
        cursor_pg = pg_conn.cursor()
        cursor_pg.execute(query_pg)
        pg_quantity_row = cursor_pg.fetchone()

        if pg_quantity_row[0] == sqlite_quantity_row[0]:
            return True
        else:
            return False


def check_equal_table(table_name: str, pg_quantity_row: list, sqlite_quantity_row: list) -> bool:

    if table_name == "person":
        for row_pg in pg_quantity_row:
            found = False
            for row_sqlite in sqlite_quantity_row:
                if row_pg[0] == row_sqlite[0] and row_pg[1] == row_sqlite[1]:
                    found = True
                    break
            if not found:
                return False
        return True
    elif table_name == "film_work":
        for row_pg in pg_quantity_row:
            found = False
            for row_sqlite in sqlite_quantity_row:
                if row_pg[0] == row_sqlite[0] and row_pg[1] == row_sqlite[1]\
                        and row_pg[2] == row_sqlite[2] and row_pg[3] == row_sqlite[3]\
                        and row_pg[5] == row_sqlite[5] and row_pg[6] == row_sqlite[6]:
                    found = True
                    break
            if not found:
                return False
        return True
    elif table_name == "genre":
        for row_pg in pg_quantity_row:
            found = False
            for row_sqlite in sqlite_quantity_row:
                if row_pg[0] == row_sqlite[0] and row_pg[1] == row_sqlite[1]\
                        and row_pg[2] == row_sqlite[2]:
                    found = True
                    break
            if not found:
                return False
        return True
    elif table_name == "person_film_work":
        for row_pg in pg_quantity_row:
            found = False
            for row_sqlite in sqlite_quantity_row:
                if row_pg[0] == row_sqlite[0] and row_pg[1] == row_sqlite[1]\
                        and row_pg[2] == row_sqlite[2] and row_pg[3] == row_sqlite[3]:
                    found = True
                    break
            if not found:
                return False
        return True
    elif table_name == "genre_film_work":
        for row_pg in pg_quantity_row:
            found = False
            for row_sqlite in sqlite_quantity_row:
                if row_pg[0] == row_sqlite[0] and row_pg[1] == row_sqlite[2]\
                        and row_pg[2] == row_sqlite[1]:
                    found = True
                    break
            if not found:
                return False
        return True


def check_table_row(table_name: str) -> bool:
    dsl = {'dbname': DB_NAME, 'user': DB_USER, 'password': DB_PASSWORD, 'host': DB_HOST, 'port': DB_PORT}
    with sqlite3.connect(DB_FILE) as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        query_sqlite = "SELECT * FROM {table};".format(table=table_name)
        cursor_sqlite = sqlite_conn.cursor()
        cursor_sqlite.execute(query_sqlite)
        sqlite_quantity_row = cursor_sqlite.fetchall()

        query_pg = "SELECT * FROM content.{table};".format(table=table_name)
        cursor_pg = pg_conn.cursor()
        cursor_pg.execute(query_pg)
        pg_quantity_row = cursor_pg.fetchall()

        return check_equal_table(table_name, pg_quantity_row, sqlite_quantity_row)


if __name__ == '__main__':
    dsl = {'dbname': DB_NAME, 'user': DB_USER, 'password': DB_PASSWORD, 'host': DB_HOST, 'port': DB_PORT}
    with sqlite3.connect(DB_FILE) as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
