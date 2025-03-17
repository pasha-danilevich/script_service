import os
import re

import pandas as pd
from settings import BASE_PATH
from utils import ExcelHandler

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

def normalize_columns(df) -> pd.DataFrame.columns:
    return df.columns.str.strip().str.upper().str.replace('\n', ' ')

def clean_string(df: pd.DataFrame) -> pd.DataFrame:
    df = (
        df['ФИО']
        .str.replace(r'[()-;]', '', regex=True)
        .str.replace(r'[\+\d]', '', regex=True)  # Удаляем "+" и цифры
        .str.strip()  # Удаляем лишние пробелы
        .str.upper()  # Преобразуем в верхний регистр
    )
    return df

def create_index_column(df):
    return df['ФИО'].str.cat(df['ДАТА РОЖДЕНИЯ'].astype(str))

if __name__ == '__main__':

    left_df = pd.read_excel(
        os.path.join(BASE_PATH, 'raznoryidka', 'dataset', 'Разнарядка_14_ВЗН_ДЕКАБРЬ_И_ЯНВАРЬ_2024г_.xlsx'),
        skiprows=0,
        # nrows=100
    )
    right_df = pd.read_excel(
        os.path.join(BASE_PATH, 'raznoryidka', 'dataset', 'разн взн февраль 2025 для аптек (2).xlsx'),
        skiprows=4,
        # nrows=100
    )
    # Применение strip и uppercase к названиям колонок
    left_df.columns = normalize_columns(left_df)
    right_df.columns = normalize_columns(right_df)

    left_df['ФИО'] = clean_string(left_df)
    left_df['INDEX_FIO_DATA'] = create_index_column(left_df)

    right_df['ФИО'] = clean_string(right_df)
    right_df['INDEX_FIO_DATA'] = create_index_column(right_df)

    result_df = pd.merge(
        left_df,  # Левый DataFrame
        right_df,  # Правый DataFrame
        how='outer',  # Тип объединения: 'inner', 'outer', 'left', 'right'
        on="INDEX_FIO_DATA",  # Столбец или список столбцов для объединения

        left_on=None,  # Столбец(ы) в левом DataFrame для объединения
        right_on=None,  # Столбец(ы) в правом DataFrame для объединения
        left_index=False,  # Использовать индекс левого DataFrame как ключ
        right_index=False,  # Использовать индекс правого DataFrame как ключ
        suffixes=('_left', '_right')  # Суффиксы для столбцов с одинаковыми именами
    )

    result_df.drop(columns=['INDEX_FIO_DATA'], inplace=True)

    eh = ExcelHandler()
    eh.write_excel(result_df, dir_name='raznoryidka', file_name='Объединение разнарядка ВЗН.xlsx')



    # Вывод названий колонок
    print(result_df[:100])





