import constants
import logging.config
import json
from dataclass_models import Person, PersonFilmwork, GenreFilmwork, Genre, Filmwork
from psycopg2.extras import DictCursor


with open(constants.LOG_FILE, 'r') as logging_config_file:
    config_dict = json.load(logging_config_file)

logging.config.dictConfig(config_dict)
logger = logging.getLogger(__name__)


class PostgresSaver:

    def __init__(self, pg_conn):
        self.pg_conn = pg_conn

    def save_row(self, cursor: DictCursor, query_write: str, values: tuple):
        try:
            with cursor as curs:
                curs.execute(query_write, values)
                self.pg_conn.commit()
                logger.info('Success!')
        except Exception as e:
            error_text = "The error {} write occurred".format(e)
            logger.error(error_text)

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
                person = Person(full_name=row['full_name'], id=row['id'],
                                created=row['created_at'], modified=row['updated_at'])
                self.save_person_to_postgres(person)
        elif name == "film_work":
            for row in table_list:
                film_work = Filmwork(id=row['id'], title=row['title'], description=row['description'],
                                     creation_date=row['creation_date'], rating=row['rating'],
                                     type=row['type'], created=row['created_at'], modified=row['updated_at'])
                self.save_film_work_to_postgres(film_work)
        elif name == "genre":
            for row in table_list:
                genre = Genre(id=row['id'], name=row['name'], description=row['description'],
                              created=row['created_at'], modified=row['updated_at'])
                self.save_genre_to_postgres(genre)
        elif name == "genre_film_work":
            for row in table_list:
                genre_film_work = GenreFilmwork(id=row['id'], film_work_id=row['film_work_id'],
                                                genre_id=row['genre_id'], created=row['created_at'])
                self.save_genre_film_work_to_postgres(genre_film_work)
        elif name == "person_film_work":
            for row in table_list:
                person_film_work = PersonFilmwork(id=row['id'], film_work_id=row['film_work_id'],
                                                  person_id=row['person_id'], role=row['role'],
                                                  created=row['created_at'])
                self.save_person_film_work_to_postgres(person_film_work)


class SQLiteLoader:

    def __init__(self, cursor):
        self.cursor = cursor

    def load_movies(self, postgres_saver: PostgresSaver):
        table_list = ["person", "film_work", "genre", "genre_film_work", "person_film_work"]
        for tablename in table_list:
            self.load_table(tablename, postgres_saver)

    def load_table(self, tablename: str, postgres_saver: PostgresSaver):
        query = "SELECT * FROM {table};".format(table=tablename)
        try:
            query_manager = self.cursor.execute(query)
            while True:
                result = query_manager.fetchmany(int(constants.UPLOAD_PORTION))
                if not result:
                    break
                else:
                    postgres_saver.save_table(tablename, result)
        except Exception as e:
            error_text = "The error {} write occurred".format(e)
            logger.error(error_text)