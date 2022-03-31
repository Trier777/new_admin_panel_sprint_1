"""Модуль тестирования."""
import pytest

from load_data import check_table_row, check_table_row_quantity


@pytest.mark.parametrize('table_name', ["genre", "film_work", "person",
                                        "genre_film_work", "person_film_work"]
                         )
def test_check_table_row_quantity(table_name: str):
    assert check_table_row_quantity(table_name), "Количество записей не совпадает!"


@pytest.mark.parametrize('table_name', ["genre", "film_work", "person",
                                        "genre_film_work", "person_film_work"]
                         )
def test_check_table_row(table_name: str):
    assert check_table_row(table_name), "Записи не совпадают!"
