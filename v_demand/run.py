import os
from pathlib import Path

import pandas as pd
from sqlalchemy import Engine

from settings import BASE_PATH, engine_single_3, engine_promed
from utils import DataReader, ExcelHandler


current_file_path = Path(__file__).resolve()
dir_name = current_file_path.parent.name


def get_df_from_sql(sql_name: str, sql_engine: Engine) -> pd.DataFrame:
    sql_path = os.path.join(BASE_PATH, dir_name, sql_name)
    data_reader = DataReader(sql_engine)
    sql_template = data_reader.read_sql_file(sql_path)
    df: pd.DataFrame = data_reader.execute_query(sql_template)
    return df

def task_three():
    df_privileges = get_df_from_sql( 'privileges_2024_2.sql', engine_promed)
    df_group = get_df_from_sql('s.sql', engine_single_3)
    df_group['Идентификатор пациента'] = pd.to_numeric(df_group['Идентификатор пациента'], errors='coerce')

    result_df = pd.merge(
        df_group,  # Левый DataFrame
        df_privileges,  # Правый DataFrame
        how='left',  # Тип объединения: 'inner', 'outer', 'left', 'right'
        on=None,  # Столбец или список столбцов для объединения
        left_on="Идентификатор пациента",  # Столбец(ы) в левом DataFrame для объединения
        right_on="person_id",  # Столбец(ы) в правом DataFrame для объединения
        left_index=False,  # Использовать индекс левого DataFrame как ключ
        right_index=False,  # Использовать индекс правого DataFrame как ключ
        suffixes=('_group', '_privileges')  # Суффиксы для столбцов с одинаковыми именами
    )

    eh = ExcelHandler()
    eh.write_excel(result_df, dir_name, file_name='patient_registry_data.xlsx')




def task_two():
    df = get_df_from_sql('s2.sql', engine_single_3)

    eh = ExcelHandler()
    eh.write_excel(df, dir_name, file_name='result.xlsx')
    ids_tuple = tuple(df["Идентификатор пациента"].tolist())

    pormed_data_reader = DataReader(engine_promed)
    sql_path = os.path.join(BASE_PATH, dir_name, 'privileg_2024.sql')
    sql_privileges_template = pormed_data_reader.read_sql_file(sql_path)
    df: pd.DataFrame = pormed_data_reader.execute_query(sql_privileges_template, {'ids': ids_tuple})

    df = df.pivot_table(index='МО', columns='Месяц 2024 года', values='Количество пациентов', aggfunc="sum",
                        fill_value=0, margins=True, margins_name='Всего').reset_index()

    eh = ExcelHandler()
    eh.write_excel(df, dir_name, file_name='Заявка Доп 2025 пациенты выкл. в рег. 2024.01.01 - 2024.12.31.xlsx')


def main():

    # task_two()

    sql_path = os.path.join(BASE_PATH, dir_name, 's.sql')

    data_reader = DataReader(engine_single_3)

    sql_template = data_reader.read_sql_file(sql_path)
    df: pd.DataFrame = data_reader.execute_query(sql_template)

    eh = ExcelHandler()
    eh.write_excel(df, dir_name, file_name='result.xlsx')





if __name__ == '__main__':
    task_three()
    # main()

