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



