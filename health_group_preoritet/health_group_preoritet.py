import shutil


import pandas as pd

import os


from sqlalchemy import text

import utils
from settings import BASE_PATH, engine
from pathlib import Path

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

current_file_path = Path(__file__).resolve()
dir_name = current_file_path.parent.name

MKB_PRIORITY_PATH = os.path.join(BASE_PATH, dir_name, 'datasets', 'Приоритизация.xlsx')

TABLE_NAME = 'temp_dm_groups'
sql_path = os.path.join(BASE_PATH, dir_name, 'health_group_preoritet.sql')

output_dir = os.path.join(BASE_PATH, dir_name, 'results')

def get_df_mkb():
    eh = utils.ExcelHandler
    df_mkb = eh.read_excel(MKB_PRIORITY_PATH, skiprows=12, usecols=(0, 1))
    mkb_name, health_group_name = df_mkb.columns.tolist()[:2]
    df_mkb = expand_mkb_10_ranges(df_mkb, mkb_name, health_group_name)
    return df_mkb




def main():

    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    Path(output_dir).mkdir(exist_ok=True)

    df_mkb = get_df_mkb()
    mkb_codes = df_mkb.loc[df_mkb['Диагноз'].str.contains('.', regex=False)]['Диагноз'].tolist()
    mkb_groups = df_mkb.loc[~df_mkb['Диагноз'].str.contains('.', regex=False)]['Диагноз'].tolist()




    with open(sql_path) as f:
        sql_template = f.read().split(';')[1]

    sql = sql_template.format(codes=tuple(mkb_codes), groups=tuple(mkb_groups))
    df_sql = pd.read_sql_query(text(sql), engine)

    # df_sql.loc['A00', 'Диагноз код']

    df_result = pd.merge(df_sql, df_mkb, how='left', left_on='Диагноз группа', right_on='Диагноз')
    df_result = pd.merge(df_result, df_mkb, how='left', left_on='Диагноз код', right_on='Диагноз')
    df_result['Группа'] = df_result['Группа_y'].fillna(df_result['Группа_x'])
    df_agg = df_result.groupby(by=['person_id', 'МО ДН', 'ФИО', 'ДР'], as_index=False).agg({'Диагноз код': set, 'Группа': set})
    df_agg['Количество пересечений'] = df_agg['Группа'].apply(len)
    df_agg = df_agg[df_agg['Количество пересечений'] > 1]
    df_agg['Диагноз код'] =  df_agg['Диагноз код'].str.join('; ')
    df_agg['Группа'] = df_agg['Группа'].str.join('; ')

    with pd.ExcelWriter(os.path.join(output_dir, f'''{"Все МО"}.xlsx''')) as writer:
        df_agg.to_excel(writer, index=False, sheet_name='A')
        writer.sheets['A'].autofit()
        writer.sheets['A'].autofilter(0, 0, df_agg.shape[0], df_agg.shape[1])

    mos = df_agg['МО ДН'].unique()
    for mo in mos:
        df_mo = df_agg[df_agg['МО ДН'] == mo]
        with pd.ExcelWriter(os.path.join(output_dir, f'''{mo.replace('"', '')}.xlsx''')) as writer:
            df_mo.to_excel(writer, index=False, sheet_name='A')
            writer.sheets['A'].autofit()
            writer.sheets['A'].autofilter(0, 0, df_mo.shape[0], df_mo.shape[1])







    # df_result.to_sql(TABLE_NAME, engine, if_exists='append', index=False)

    # df_result.loc[df_result['Диагноз код'].str.startswith(df_result['Диагноз_y'].str), 'Группа входит в код'] = df_result['Группа_y']
    # print(df_sql[df_sql['Диагноз группа'] == 'N18'][100])
    # df_result['Группа входит в код'] = df_result['Диагноз группа'] == 'N18.9'

# def starter(mkb_codes, mkb_groups):
#     with open(sql_path) as f:
#         sql_template = f.read().split(';')[0]
#
#     sql = sql_template.format(codes=tuple(mkb_codes), groups=tuple(mkb_groups))
#     with engine.connect() as con:
#
#         count = con.execute(sql).first()[0]
#         print(count)

main()


