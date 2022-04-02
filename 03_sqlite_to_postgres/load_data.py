import sqlite3
import psycopg2 as psycopg2
from psycopg2.extras import DictCursor
from contextlib import contextmanager
from loader import SQLiteLoader, PostgresSaver
import logging.config
import constants


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: psycopg2.extensions.connection):
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_loader = SQLiteLoader(connection)
    sqlite_loader.load_movies(postgres_saver)


@contextmanager
def open_db(file_name: str):
    conn = sqlite3.connect(file_name)
    conn.row_factory = sqlite3.Row
    try:
        logging.info("Creating connection")
        yield conn.cursor()
    finally:
        logging.info("Closing connection")
        conn.commit()
        conn.close()


if __name__ == '__main__':
    with open_db(constants.DB_FILE) as sqlite_conn,\
            psycopg2.connect(**constants.DSL, cursor_factory=DictCursor) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
