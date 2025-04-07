import os
import re
from datetime import date, timedelta
from pathlib import Path
from pprint import pprint

import numpy as np
import pandas as pd

from sqlalchemy import text

from settings import engine, BASE_PATH

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)


# value * population_count / number_day_in_year
# 1 * 93847 / 335 = 280.140 = 'смертность 35604000 - Бахчисарайский муниципальный район'
# print(f"{today:%j}")  # номер дня в году с 1

def get_creamea_population(year: str, region: str) -> int:
    sql = text(f"""
    select
    sum(Значение) as "Население {year}" 
    from "popul-creamea-stat"
    where "Пол" = 'Оба пола' 
    and "Население" = 'Все' 
    and "Возраст" between 0 and 150
    and Регион = '{region}'
    and Год = '{year}'
    """)
    df = pd.read_sql_query(sql, engine.connect())
    return df[f"Население {year}"][0]


def extract_month(date_str):
    # Используем регулярное выражение для поиска месяца в строке
    month_name = date_str.split()[2].split('_')[1].lower()

    # Словарь для преобразования названия месяца в числовой формат
    month_dict = {
        'январь': 1,
        'февраль': 2,
        'март': 3,
        'апрель': 4,
        'май': 5,
        'июнь': 6,
        'июль': 7,
        'август': 8,
        'сентябрь': 9,
        'октябрь': 10,
        'ноябрь': 11,
        'декабрь': 12
    }

    return month_dict[month_name]  # Возвращаем числовой формат месяца


def get_last_day_of_month(_year, _month):
    # Получаем первый день следующего месяца
    if _month == 12:
        next_month = date(_year + 1, 1, 1)
    else:
        next_month = date(_year, _month + 1, 1)

    # Вычитаем один день, чтобы получить последний день текущего месяца
    last_day_of_month = next_month - timedelta(days=1)

    # Возвращаем номер дня в году
    return last_day_of_month.timetuple().tm_yday


def get_files(directory):
    file_names = []
    for entry in os.listdir(directory):
        full_path = os.path.join(directory, entry)
        if os.path.isfile(full_path):
            file_names.append(entry)
    return file_names


def get_current_dir_name():
    module_path = os.path.dirname(os.path.abspath(__file__))

    # Получаем имя директории
    directory_name = os.path.basename(module_path)
    return directory_name


def get_df(path: Path, skip: int = 0, sheet_index: int = 0) -> pd.DataFrame:
    print(path)
    xl = pd.ExcelFile(path)

    df_ = pd.read_excel(
        xl,
        sheet_name=xl.sheet_names[sheet_index],
        skiprows=skip,
    )

    return df_


def get_db_all_regions() -> list[str]:
    sql = """SELECT DISTINCT pcs."Регион"
    FROM public."popul-creamea-stat" AS pcs
    ORDER BY "Регион"
    """
    df_ = pd.read_sql_query(sql, engine.connect())
    regions_list = df_["Регион"].tolist()

    return regions_list


def get_municipality_name_dict(municipalities: list[str], region_names: list[str]):
    # Создаем словарь
    municipality_dict = {}

    # Сопоставляем элементы
    for municipality in municipalities:
        # Извлекаем название района/округа
        name = municipality.split(' - ')[1].strip().replace(
            "муниципальный район", "").replace("Городской округ", "").strip()

        # Ищем подходящее значение из второго списка
        for value in region_names:
            if name.upper() == value:
                municipality_dict[municipality] = value
                break

    return municipality_dict


def save_to_excel(filename, df, output=''):
    with pd.ExcelWriter(os.path.join(output, f"{filename}")) as wr:
        df.to_excel(wr, sheet_name='A', index=False)

        # wr.sheets['A'].autofit()
        # wr.sheets['A'].autofilter(0, 0, df.shape[0], df.shape[1])


def main():
    path_datasets = Path(BASE_PATH / get_current_dir_name() / 'datasets')
    file = Path(path_datasets / '2024_МСн_д_35_январь_декабрь_2024_20250123_161515.xlsx')
    year = 2024

    df = get_df(file, skip=11, sheet_index=2)

    # month_num = extract_month(str(file))
    # number_day_in_year = get_last_day_of_month(year, month_num)
    municipalities: list[str] = list(df.columns)[4:29]
    all_regions_names = get_db_all_regions()
    municipality_name_dict = get_municipality_name_dict(municipalities, all_regions_names)

    column_number = 5
    for municipality, region_name in municipality_name_dict.items():
        population_count: int = get_creamea_population(year=str(year), region=region_name)
        # value * 100_000 / population_count / number_day_in_year
        values = round(df[municipality][2:] * 100_000 / population_count, 1)

        print(population_count, region_name)

        df.insert(loc=column_number, column=f'Смертность {municipality}', value=values)
        column_number += 2

    values = round(df['Всего'] * 100_000 / 1_909_499, 1)
    df.insert(loc=4, column=f'Смертность - Всего', value=values)

    save_to_excel(f'Смертность {year} по муниципальным образованиям.xlsx', df)


def main_2():
    path_datasets = Path(BASE_PATH / get_current_dir_name() / 'datasets')
    path_correct_rows = Path(path_datasets / 'Смертность-нужные строки.xlsx')
    path_mortality = Path(path_datasets.parent / 'Смертность 2024 по муниципальным образованиям.xlsx')

    df_correct_rows_orig = get_df(path_correct_rows, skip=1)
    df_correct_rows = df_correct_rows_orig[['Нозология', 'код строки', 'КРЫМ (12.2023)']]
    code_rows = df_correct_rows['код строки']

    df_mortality = get_df(path_mortality)
    df_mortality = df_mortality[df_mortality['Код строки'].isin(code_rows)]
    df_mortality.drop(0, inplace=True)
    df_mortality.rename(columns={'Код строки': 'код строки'}, inplace=True)

    columns = list(df_mortality.columns)
    # 1. Убираем колонки с "Смертность"
    filtered_columns = [col for col in columns if '.1' not in col]

    df_mortality_absolute_columns = ['код строки'] + [col for col in filtered_columns if 'Смертность' not in col][
                                                     4:] + ['Всего']
    df_mortality_absolute = df_mortality[df_mortality_absolute_columns]

    # df_mortality[[col for col in filtered_columns if 'Смертность' in col]] *= 366

    df_mortality_percent = df_mortality[['код строки'] + [col for col in filtered_columns if 'Смертность' in col]]

    for col in df_mortality_absolute.columns:
        try:
            df_mortality_absolute.rename(columns={col: col.split('-')[1]}, inplace=True)
        except IndexError:
            pass
    df_mortality_absolute.rename(columns={'Всего': 'Крым 2024'}, inplace=True)

    for col in df_mortality_percent.columns:
        try:
            df_mortality_percent.rename(columns={col: col.split('-')[1].strip()}, inplace=True)
        except IndexError:
            pass
    df_mortality_percent.rename(columns={'Всего': 'Крым 2024'}, inplace=True)

    # # print(df_mortality_absolute[:5])
    # print(df_mortality_percent.columns)

    result_df_mortality_absolute = pd.merge(
        df_correct_rows,  # Левый DataFrame
        df_mortality_absolute,  # Правый DataFrame
        how='left',  # Тип объединения: 'inner', 'outer', 'left', 'right'
        on='код строки',  # Столбец или список столбцов для объединения
    )
    # Получаем список колонок
    cols = result_df_mortality_absolute.columns.tolist()

    # Удаляем 3-ю колонку (индекс 2, т.к. Python нумерует с 0)
    col_to_move = cols.pop(2)  # 'C'

    # Добавляем её в конец
    cols.append(col_to_move)

    # Пересоздаём DataFrame с новым порядком
    result_df_mortality_absolute = result_df_mortality_absolute[cols]
    result_df_mortality_absolute['Δ, абс.'] = round(
        result_df_mortality_absolute['Крым 2024'] - result_df_mortality_absolute['КРЫМ (12.2023)'], 2)
    result_df_mortality_absolute["Δ, %"] = round(
        (result_df_mortality_absolute['Крым 2024'] - result_df_mortality_absolute['КРЫМ (12.2023)']) /
        result_df_mortality_absolute['КРЫМ (12.2023)'] * 100, 2)

    result_df_mortality_percent = pd.merge(
        df_correct_rows,  # Левый DataFrame
        df_mortality_percent,  # Правый DataFrame
        how='left',  # Тип объединения: 'inner', 'outer', 'left', 'right'
        on='код строки',  # Столбец или список столбцов для объединения
    )

    df_correct_rows_orig_percent = get_df(path_correct_rows, skip=1, sheet_index=1)

    # Получаем список колонок
    cols = result_df_mortality_percent.columns.tolist()

    # Удаляем 3-ю колонку (индекс 2, т.к. Python нумерует с 0)
    col_to_move = cols.pop(2)  # 'C'

    # Добавляем её в конец
    cols.append(col_to_move)
    col_to_move = cols.pop(2)
    cols.append(col_to_move)
    print(df_correct_rows_orig_percent['КРЫМ (12.2023)'][:5])
    # result_df_mortality_percent['Крым 2023'] = df_correct_rows_orig_percent['КРЫМ (12.2023)']
    # Пересоздаём DataFrame с новым порядком
    print(cols)
    result_df_mortality_percent = result_df_mortality_percent[cols]
    result_df_mortality_percent['КРЫМ (12.2023)'] = df_correct_rows_orig_percent['КРЫМ (12.2023)']
    result_df_mortality_percent['Δ, абс.'] = round(
        result_df_mortality_percent['Крым 2024'] - result_df_mortality_percent[
            'КРЫМ (12.2023)'], 2)
    result_df_mortality_percent["Δ, %"] = round((result_df_mortality_percent['Крым 2024'] - result_df_mortality_percent[
        'КРЫМ (12.2023)']) / result_df_mortality_percent['КРЫМ (12.2023)'] * 100, 2)

    print(result_df_mortality_percent[:5])
    with pd.ExcelWriter(f'Смертность 2024 по муниципальным образованиям (нужные коды).xlsx') as writer:
        result_df_mortality_absolute.to_excel(writer, sheet_name='Умерших', index=False)
        result_df_mortality_percent.to_excel(writer, sheet_name='Смертность', index=False)

        writer.sheets['Умерших'].autofit()
        writer.sheets['Умерших'].autofilter(0, 0, result_df_mortality_percent.shape[0],
                                            result_df_mortality_percent.shape[1])

        writer.sheets['Смертность'].autofit()
        writer.sheets['Смертность'].autofilter(0, 0, result_df_mortality_percent.shape[0],
                                               result_df_mortality_percent.shape[1])


if __name__ == '__main__':
    main()
    main_2()