import os

import numpy as np
import pandas as pd
from docxtpl import DocxTemplate
from sqlalchemy import text

from settings import BASE_PATH, engine_promed

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)


def get_recipe_emis(sn: tuple) -> pd.DataFrame:
    def construct(sn: tuple) -> str:
        return f"= '{sn[0]}'" if len(sn) < 2 else f'in {sn}'

    oper = construct(sn)
    sql = text(f"""
    select 
    (r.evnrecept_ser || ' ' || r.evnrecept_num) as sn,
    to_char(r.evn_setdt, 'YYYY-MM-DD') as "Дата рецепта ЕМИС",
    coalesce(rdt.receptdelaytype_name, 'Выписан') as status
    from evnrecept r
    left join receptdelaytype rdt on r.receptdelaytype_id = rdt.receptdelaytype_id 
    where (r.evnrecept_ser || ' ' || r.evnrecept_num) {oper}
    """)
    with engine_promed.connect() as conn:
        df = pd.read_sql_query(sql, conn)
    print(df.head())
    return df


# get_recipe_emis(sn=('35303 541179', ))

def get_skiprow(file):
    df = pd.read_excel(file, usecols='A', names=['A'])
    try:
        skip = df[df['A'].str.contains('№ п/п', na=False)].index[0] + 1
    except IndexError:
        print("except")
        skip = df[df['A'].str.contains('№', na=False)].index[0] + 1
    print(skip)
    return skip


def prepare_file(file):
    xl = pd.ExcelFile(file)

    df = pd.read_excel(xl,
                       sheet_name=xl.sheet_names[0],
                       skiprows=get_skiprow(xl),
                       usecols=['Серия и номер рецепта', 'Дата отпуска', 'ФИО пациента'],
                       dtype=str)
    df = df[~df['ФИО пациента'].isin(['NaN', np.nan, None, 'None'])]

    df.rename(columns={
        'Серия и номер рецепта': 'sn'
    }, inplace=True)


    # Убираем первую строку, так как она содержит заголовки для новых колонок
    # df = df[1:].reset_index(drop=True)

    df['sn'].dropna(inplace=True)
    df.drop(index=[0], inplace=True)

    df = df[~df['sn'].isin(['Nan', np.nan, None, 'None'])  ]
    print(df[:3])
    sn = tuple(df['sn'].unique())  # [0: 100]

    return df, sn


def save_to_excel(filename, df, df_agg, output):
    df_dict = {
        'Свод': df_agg,
        "Список": df
    }
    f = f"Проверено {filename}"
    print(f)
    with pd.ExcelWriter(os.path.join(output, f)) as wr:
        for name, df in df_dict.items():
            df.to_excel(wr, sheet_name=name, index=False)
            worksheet = wr.sheets[name]
            worksheet.autofit()
            worksheet.autofilter(0, 0, df.shape[0], df.shape[1])


def prep_to_docs(filename, df):
    row_dict = {}
    all_ = df['sn'].sum()
    row_dict['reestr'] = filename
    row_dict['all'] = all_
    row_dict['data'] = df.to_dict(orient='records')
    return row_dict


from datetime import date
import shutil
import pathlib


def read_files():
    dir_name = 'Рецепты'
    output = os.path.join(BASE_PATH, 'krimfarma_reestrs', f"Результаты {date.today()} {dir_name}")
    if os.path.exists(output):
        shutil.rmtree(output)
    pathlib.Path(output).mkdir(parents=True, exist_ok=True)

    # doc = DocxTemplate(os.path.join(BASE_PATH, 'promed', 'krimfarma_reestrs', 'templates', 'reestr_template.docx'))
    # path = '/home/andrey/PycharmProjects/scripts-service/promed/krimfarma_reestrs/datasets/test'
    doc = DocxTemplate(os.path.join(BASE_PATH, 'krimfarma_reestrs', 'templates', 'reestr_template.docx'))
    path = os.path.join(BASE_PATH, 'krimfarma_reestrs', 'datasets', 'Для КМИАЦ 23.10.2024', dir_name)
    files = [os.path.join(path, filename) for filename in os.listdir(path)]
    row_list = []
    # Рекурсивно обходим все директории и файлы

    for filename in files:
        if filename.endswith('.xlsx'):

            file_path = filename
            filename = filename.split('/')[-1]
            print(f"Processing file: {file_path}")



            df, sn = prepare_file(file_path)


            print('----------')
            dfr = get_recipe_emis(sn)

            df = df.merge(dfr, on='sn', how='left')
            df = df[~df['status'].isin(['NaN', np.nan, None, 'None'])]

            df_agg = df.groupby(by=['status'], as_index=False)['sn'].nunique()

            save_to_excel(filename=filename, df=df, df_agg=df_agg, output=output)

            row_dict = prep_to_docs(filename, df_agg)
            row_list.append(row_dict)


        context = {'rows': row_list}

        doc.render(context)
        doc.save(os.path.join(output, f"Реестры ответ на {date.today()}.docx"))



if __name__ == '__main__':
    read_files()
