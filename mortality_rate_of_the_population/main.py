import os
from datetime import date, timedelta
from pathlib import Path
from pprint import pprint

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


def get_df(path):
    print(path)
    xl = pd.ExcelFile(path)

    df_ = pd.read_excel(
        xl,
        sheet_name=xl.sheet_names[2],
        skiprows=11,
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
    files_names = [item for item in get_files(path_datasets) if "~" not in item]
    year = 2024

    for file_name in files_names:
        df = get_df(path_datasets / file_name)

        month_num = extract_month(file_name)
        number_day_in_year = get_last_day_of_month(year, month_num)

        municipalities: list[str] = list(df.columns)[4:29]
        all_regions_names = get_db_all_regions()
        municipality_name_dict = get_municipality_name_dict(municipalities, all_regions_names)

        # value * population_count / number_day_in_year

        column_number = 5
        print(number_day_in_year)
        for municipality, region_name in municipality_name_dict.items():
            population_count: int = get_creamea_population(year=str(year), region=region_name)
            values = round(df[municipality][2:] * 100000 / population_count / number_day_in_year, 4)

            print(population_count, region_name)


            df.insert(loc=column_number, column=f'Смертность {municipality}', value=values)
            column_number += 2

            save_to_excel(f'Смертность {year} по муниципальным образованиям.xlsx', df)






if __name__ == '__main__':
    main()
