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


def get_df(path, skiprows: int):
    xl = pd.ExcelFile(path)

    df_ = pd.read_excel(
        xl,
        sheet_name=xl.sheet_names[0],
        skiprows=skiprows,
    )

    return df_


def save_to_excel(filename, df, output=''):
    with pd.ExcelWriter(os.path.join(output, f"{filename}")) as wr:
        df.to_excel(wr, sheet_name='A', index=False)

        wr.sheets['A'].autofit()
        wr.sheets['A'].autofilter(0, 0, df.shape[0], df.shape[1])


def main():
    path_datasets = Path(BASE_PATH / get_current_dir_name() / 'dataset')

    df_left = get_df(path_datasets / 'слева.xlsx', 0)
    df_right = get_df(path_datasets / 'справа.xlsx', 1)

    df_left = df_left.drop(df_left.columns[[0, 1]], axis=1)

    df_left = df_left.dropna(subset=['ФИО'])
    df_right = df_right.dropna(subset=['ФИО'])

    df_left = df_left[df_left['Посмертный диагноз'].notna()]
    df_left = df_left[
        df_left['Посмертный диагноз'].str.startswith('U07.0')
        | df_left['Посмертный диагноз'].str.startswith('U07.1')]

    # ---
    # value = df_left['ФИО'].isin(df_right['ФИО']).map({True: 'да', False: 'нет'})
    # df_left.insert(loc=6, column=f'Содержит', value=value)
    #
    # save_to_excel(f'Сочетание по ФИО.xlsx', df_left)
    # print('done Сочетание по ФИО.xlsx')
    # ---

    # Преобразование столбца 'Дата рождения' в формат datetime
    df_left['Дата рождения'] = pd.to_datetime(df_left['Дата рождения'], dayfirst=True, errors='coerce').dt.date
    df_right['Дата рождения'] = pd.to_datetime(df_right['Дата рождения'], dayfirst=True, errors='coerce').dt.date

    # Создание уникального индекса на основе ФИО и даты рождения
    df_left['INDEX'] = (df_left['ФИО'] + df_left['Дата рождения'].astype(str)).str.upper().str.replace(' ',
                                                                                                       '').str.strip()
    df_right['INDEX'] = (df_right['ФИО'] + df_right['Дата рождения'].astype(str)).str.upper().str.replace(' ',
                                                                                                          '').str.strip()

    # Проверка наличия индекса из df_left в df_right
    value = df_left['INDEX'].isin(df_right['INDEX']).map({True: 'да', False: 'нет'})

    # Вставка результата в df_left
    df_left.insert(loc=4, column='Содержит', value=value)
    print(df_left.head(10))

    df_left = df_left.drop(columns=['INDEX'])
    save_to_excel(f'Сочетание по ФИО+ДР.xlsx', df_left)


if __name__ == '__main__':
    main()
