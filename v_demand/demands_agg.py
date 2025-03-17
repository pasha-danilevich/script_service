import os
from pathlib import Path

import pandas as pd

from settings import BASE_PATH, engine_promed
from utils import Repository, ExcelHandler

current_file_path = Path(__file__).resolve()
dir_name = current_file_path.parent.name


def task_two(df: pd.DataFrame):
    df = df.pivot_table(index='МО', columns='Выписка рецептов 2024', values='ФИО', aggfunc="nunique",
                        fill_value=0, margins=True, margins_name='Всего').reset_index()
    df = df.rename(columns={'ФИО': 'Количество пациентов'})

    return df

def main():

    sql_path = os.path.join(BASE_PATH, dir_name, 'demand_2025_death.sql')

    data_reader = Repository(engine_promed)

    sql_template = data_reader.read_sql_file(sql_path)
    df_full: pd.DataFrame = data_reader.execute_query(sql_template)
    # eh = ExcelHandler()
    # eh.write_excel(df_full, dir_name, file_name='Вписка рецептов 2024.xlsx')
    df_agg = task_three(df_full)

    dict_df = {'Полный список': df_full, 'Агрегация 2025 2024': df_agg}
    file_name = 'Заявка 2025 план умершие.xlsx'

    with pd.ExcelWriter(file_name) as writer:

        for name_, df_  in dict_df.items():
            df_.to_excel(writer,sheet_name=name_, index=False)
            writer.sheets[name_].autofit()
            writer.sheets[name_].autofilter(0, 0, df_.shape[0], df_.shape[1])




def task_three(df):

    df = df.pivot_table(index='МО', columns='Дата смерти', values="Идентификатор пациента", aggfunc="nunique",
                        fill_value=0, margins=True, margins_name='Всего').reset_index()
    df = df.rename(columns={"Идентификатор пациента": 'Количество пациентов'})

    return df



if __name__ == '__main__':

    main()

