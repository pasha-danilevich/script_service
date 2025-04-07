import warnings
from pathlib import Path
from typing import cast

import pandas as pd
from transliterate import translit

from settings import BASE_PATH


warnings.simplefilter(action="ignore", category=FutureWarning)



pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

def convert_to_latin(input_string) -> str:
    """
    Проверяет входную строку на наличие кириллических символов и переводит их в латиницу.

    :param input_string: Входная строка для проверки и возможного преобразования.
    :return: Строка с кириллическими символами заменёнными на латинские или исходная строка, если она была полностью на латыни.
    """

    # Проверка на наличие кириллических символов
    if any('а' <= c <= 'я' or 'А' <= c <= 'Я' for c in input_string):
        # Преобразование в транслитерацию (латиницу)
        latin_string = translit(input_string, 'ru', reversed=True)
        return latin_string

    # Если нет кириллических символов — возвращаем исходную строку
    return input_string


def expand_mkb_10_ranges(df: pd.DataFrame, mkb_column_name: str, health_group_name: str) -> pd.DataFrame:
    df[mkb_column_name] = df[mkb_column_name].str.strip()

    # Фильтрация значений в столбце 'код', длина которых больше N
    range_codes = df[df[mkb_column_name].str.contains('-')]

    indexes_to_remove = df[df[mkb_column_name].str.contains('-')].index
    df.drop(indexes_to_remove, inplace=True)

    expanded_rows = []
    for index, row in range_codes.iterrows():

        raw_range: str = convert_to_latin(cast(str, row[0]))  # D00-D09, D80.0-D80.9

        number_type = 'float' if '.' in raw_range else 'int'

        health_group = row[1]  # I, II, etc

        letter = raw_range[0]  # A, B, D etc
        start, end = raw_range.split("-", 1)  # D00, D09
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
            range_list = [f'{letter}{raw_range[1]}{num}' if num <= 9 else f'{letter}{num}' for num in
                          range(r_start, r_start + range_)]
        elif number_type == 'float':
            range_list = [f'{letter}{raw_range[1:3]}.{num}' for num in range(range_)]

        for code in range_list:
            expanded_rows.append([code, health_group])

    df_expanded = pd.DataFrame(expanded_rows, columns=[mkb_column_name, health_group_name])

    df = pd.concat([df, df_expanded], axis=0)

    return df


def read_mkb_file():
    mkb_10_column = "код МКБ 10"
    mkb_health_group_column = "группа здоровья"
    path = Path(BASE_PATH, 'health_group_check', 'datasets', 'ГРУППЫ_ЗДОРОВЬЯ 1.1.xlsx')
    df = pd.read_excel(path)

    df = expand_mkb_10_ranges(df, mkb_10_column, mkb_health_group_column)
    return df



if __name__ == '__main__':
    df = read_mkb_file()
    print(df)