import sqlite3
import psycopg2 as psycopg2
import constants
from psycopg2.extras import DictCursor


def check_table_row_quantity(table_name: str) -> bool:
    with sqlite3.connect(constants.DB_FILE) as sqlite_conn, psycopg2.connect(**constants.DSL,
                                                                             cursor_factory=DictCursor) as pg_conn:
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
    with sqlite3.connect(constants.DB_FILE) as sqlite_conn,\
            psycopg2.connect(**constants.DSL, cursor_factory=DictCursor) as pg_conn:
        query_sqlite = "SELECT * FROM {table};".format(table=table_name)
        cursor_sqlite = sqlite_conn.cursor()
        cursor_sqlite.execute(query_sqlite)
        sqlite_quantity_row = cursor_sqlite.fetchall()

        query_pg = "SELECT * FROM content.{table};".format(table=table_name)
        cursor_pg = pg_conn.cursor()
        cursor_pg.execute(query_pg)
        pg_quantity_row = cursor_pg.fetchall()

        return check_equal_table(table_name, pg_quantity_row, sqlite_quantity_row)
