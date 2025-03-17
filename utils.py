import os
import warnings
from pathlib import Path
from typing import cast

import pandas as pd
from sqlalchemy import text

from settings import BASE_PATH


class ExcelHandler:
    def __init__(self):
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', None)


    @staticmethod
    def read_excel(file_path, **kwargs):
        warnings.simplefilter(action="ignore", category=FutureWarning)
        return pd.read_excel(file_path, **kwargs)

    @staticmethod
    def write_excel(df: pd.DataFrame, dir_name: str, file_name: str = None, index: bool = False):
        file_name = file_name if file_name else 'noname.xlsx'
        file_path = os.path.join(BASE_PATH, dir_name, 'result', file_name)
        # TODO: add "result" folder
        with pd.ExcelWriter(file_name) as wr:
            df.to_excel(wr, index=index, sheet_name='A')
            wr.sheets['A'].autofit()
            wr.sheets['A'].autofilter(0, 0, df.shape[0], df.shape[1])


class Repository:
    def __init__(self, engine):
        """
        Инициализация класса.
        :param engine: SQLAlchemy движок для подключения к базе данных.
        """
        self.engine = engine

        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', None)


    @staticmethod
    def read_sql_file(sql_path: Path):
        """
        Чтение SQL-запроса из файла.
        :param sql_path: Путь к sql файлу
        :return: Строка с SQL-запросом.
        """

        try:
            with open(sql_path, 'r') as f:
                return f.read()
        except FileNotFoundError:
            print(f"Файл {sql_path} не найден.")
            return None



    def execute_query(self, sql_template, condition: dict = None) -> pd.DataFrame | None:
        """
        Выполнение SQL-запроса с возможностью подстановки условия.

        :param sql_template: Шаблон SQL-запроса или строка запроса напрямую.
        :param condition: Условие для подстановки в шаблон. По умолчанию None.
        :return: DataFrame с результатами запроса или None при ошибке.
        """

        # Если передан шаблон — подставляем условие
        if isinstance(sql_template, str) and '{' in sql_template and condition is not None:
            sql = sql_template.format(**condition)

            df = self.__get_df_from_connection(sql)
            return df

        # Если передана готовая строка запроса — выполняем её напрямую без замены условий
        elif isinstance(sql_template, str) and '{' not in sql_template or condition is None:
            df = self.__get_df_from_connection(sql_template)
            return df

        # Если что-то пошло не так — возвращаем ошибку
        else:
            print("Неправильный формат входных данных.")
            print(sql_template)
            return None

    def __get_df_from_connection(self, sql) -> pd.DataFrame | None:
        try:
            with self.engine.connect() as con:
                df = pd.read_sql_query(text(sql), con=con)
                return df
        except Exception as e:
            print(f"Ошибка при выполнении запроса: {e}")
            return None




def expand_mkb_10_ranges(df: pd.DataFrame, mkb_column_name: str, health_group_name: str) -> pd.DataFrame:

    df[mkb_column_name] = df[mkb_column_name].str.strip()

    # Фильтрация значений в столбце 'код', длина которых больше N
    range_codes = df[df[mkb_column_name].str.contains('-')]

    indexes_to_remove = df[df[mkb_column_name].str.contains('-')].index
    df.drop(indexes_to_remove, inplace=True)

    expanded_rows = []
    for index, row in range_codes.iterrows():

        raw_range: str = convert_to_latin(cast(str, row[0])) # D00-D09, D80.0-D80.9

        number_type = 'float' if '.' in raw_range else 'int'

        health_group = row[1] # I, II, etc

        letter = raw_range[0] # A, B, D etc
        start, end = raw_range.split("-", 1) # D00, D09
        if letter == 'D':
            print()
        if number_type == 'int':
            start = int(start[1:])
            end = int(end[1:])
        elif number_type == 'float':
            start = int(start[-1])
            end = int(end[-1])

        range_ = end - start + 1
        if raw_range == 'I48-I49':
            print(start, end)
        range_list = []
        if number_type == 'int':
            r_start = start % 10
            range_list = [f'{letter}{raw_range[1]}{num}' if num <= 9 else f'{letter}{num}' for num in range(r_start, r_start+range_)]
        elif number_type == 'float':
            range_list = [f'{letter}{raw_range[1:3]}.{num}' for num in range(range_)]



        for code in range_list:
            expanded_rows.append([code, health_group])

    df_expanded = pd.DataFrame(expanded_rows, columns=[mkb_column_name, health_group_name])

    df = pd.concat([df, df_expanded], axis=0)

    return df


from transliterate import translit


def convert_to_latin(input_string) -> str | None:
    """
    Проверяет входную строку на наличие кириллических символов и переводит их в латиницу.

    :param input_string: Входная строка для проверки и возможного преобразования.
    :return: Строка с кириллическими символами заменёнными на латинские или исходная строка, если она была полностью на латыни.
    """

    # Проверка на наличие кириллических символов
    if any('а' <= c <= 'я' or 'А' <= c <= 'Я' for c in input_string):
        try:
            # Преобразование в транслитерацию (латиницу)
            latin_string = translit(input_string, 'ru', reversed=True)
            return latin_string
        except Exception as e:
            print(f"Ошибка при преобразовании строки: {e}")
            return None  # или любое другое поведение по умолчанию

    # Если нет кириллических символов — возвращаем исходную строку
    return input_string


