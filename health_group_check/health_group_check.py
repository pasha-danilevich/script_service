import re
from typing import cast

import pandas as pd
import warnings
import os
from dotenv import load_dotenv
from sqlalchemy import text
from settings import engine_promed, BASE_PATH
from utils import expand_mkb_10_ranges

# from settings import engine_promed

warnings.simplefilter(action="ignore", category=FutureWarning)

load_dotenv()

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


mkb_10_column = "код МКБ 10"
mkb_health_group_column = "группа здоровья"

MKB_PATH = os.path.join(BASE_PATH, 'health_group_check', 'datasets', 'ГРУППЫ_ЗДОРОВЬЯ 1.1.xlsx')
GROUP_CSV_PATH =  os.path.join(BASE_PATH, 'health_group_check', 'datasets', 'Result_9.csv')


def read_mkb_file():

    path = MKB_PATH
    df = pd.read_excel(path)

    df = expand_mkb_10_ranges(df, mkb_10_column, mkb_health_group_column)
    print(df)
    return df

def read_data_file_sql(condition):
    sql_file = os.path.join(BASE_PATH, 'health_group_check', 'health_query.sql')
    with open(sql_file, 'r') as f:
        sql_template = f.read()

    # Подставляем условие в SQL-запрос
    sql = sql_template.format(condition=condition)

    with engine_promed.connect() as con:
        df = pd.read_sql_query(text(sql), con=con)

    df = normalize_group(df)
    return df

def normalize_group(df: pd.DataFrame) -> pd.DataFrame:
    df['Группа здоровья'] = df['Группа здоровья'].str.upper()
    return df

def read_data_file_csv():

    path = GROUP_CSV_PATH
    df = pd.read_csv(path)
    df = normalize_group(df)

    # print(df.head())
    return df

# def read_file_prof():
#     check_df = read_file_check()
#     mkb_df = read_mkb_file()
#     print(check_df[:10])
#
#
#
#     df['Группа здоровья'] = df['Группа здоровья'].str.upper()
#     df = df[df['Диагноз'] != 'Z00.0']
#
#     for row in check_df:
#
#         ds_code = row.get('код МКБ 10')
#         health_group = row.get('группа здоровья')
#         df.loc[df['Диагноз'].str.contains('|'.join(ds_code), na=False), 'Группа здоровья из справочника'] =  health_group
#
#         # df['Соответсвует справочнику'].fillna('Нет', inplace=True)
#
#
#     df.loc[df['Группа здоровья'] == df['Группа здоровья из справочника'], 'Соответсвует справочнику'] = "Да"
#     df['Соответсвует справочнику'].fillna('Нет', inplace=True)

def make_hash_map(df: pd.DataFrame) -> dict[str, str]:
    hash_map: dict[str, str] = {}

    for _, row in df.iterrows():
        hash_map[row[mkb_10_column]] = row[mkb_health_group_column]

    return hash_map



def write_result(df: pd.DataFrame) -> None:
    with pd.ExcelWriter('Анализ соответствия групп здоровья профы взрослые.xlsx') as wr:
        df.to_excel(wr, index=False, sheet_name='A')
        wr.sheets['A'].autofit()
        wr.sheets['A'].autofilter(0, 0, df.shape[0], df.shape[1])

def check_diagnosis(row, hash_map):
    diagnosis = row['Диагноз']
    health_group = row['Группа здоровья']

    # Проверяем, есть ли диагноз в hash_map и совпадает ли группа здоровья
    if diagnosis in hash_map:
        correct_group = hash_map[diagnosis]
        if correct_group == health_group:
            return "Да", correct_group  # Группа здоровья совпадает
        else:
            return "Нет", correct_group  # Группа здоровья не совпадает
    else:
        # Диагноз отсутствует в справочнике, по ТЗ выставляем группу I
        # и проверяем равна ли группа здоровья 'I'
        return "Да" if health_group == 'I' else "Нет", 'I'


def proces_check():
    # noinspection SpellCheckingInspection
    condition = "d.evnpldispprof_setdt between '2025-01-01' and current_date"
    hash_map = make_hash_map(read_mkb_file())
    # df = read_data_file_sql(condition=condition)
    #
    #
    # # Применяем функцию и получаем статус и правильную группу здоровья
    # results = df.apply(lambda row: check_diagnosis(row, hash_map), axis=1)
    #
    # # Разделяем результаты на два столбца
    # df['Соответствие МКБ 10'] = results.apply(lambda x: x[0])  # Статус ("Да", "Нет", "Отсутствует")
    # df['Правильная группа здоровья'] = results.apply(lambda x: x[1])  # Группа здоровья


    # write_result(df)



proces_check()

