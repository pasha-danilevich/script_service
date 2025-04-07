from pathlib import Path

import pandas as pd

from mkb_dn.utils import expand_range, code_type
from settings import engine_single_3

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


def make_df(data: list[tuple]) -> None:
    columns = ['code', 'type',
               'multiplicity',
               'indicator',
               'indicator_multiplicity',
               'usl',
               'usl_code',
               ]

    df = pd.DataFrame(data, columns=columns)

    # Сохранение DataFrame в Excel-файл
    df.to_excel('output.xlsx', index=False)


def get_data_from_excel(sheet_name: str, xl: pd.ExcelFile) -> list[tuple]:
    df = pd.read_excel(
        xl,
        sheet_name=sheet_name,
        skiprows=2,
    )

    df['Кратность'] = df['Кратность'].ffill()
    df.drop([0, 1], inplace=True, axis='rows')

    # Заполнение NaN в колонке "Кратность.1" предыдущим значением
    df['Контролируемые показатели'] = df['Контролируемые показатели'].ffill()
    df['Кратность.1'] = df['Кратность.1'].ffill()
    df['Наименование медицинской услуги'] = df['Наименование медицинской услуги'].ffill()
    df['Код услуги'] = df['Код услуги'].ffill()

    last_columns = df.iloc[:, -5:]
    tuples_list = [tuple(row) for row in last_columns.values]

    items = []
    for t in tuples_list:
        item = (
            sheet_name,  # Код
            *t  # Разворачиваем кортеж t
        )
        items.append(item)

    return items


def to_sql_single_3() -> None:
    file_path = Path('output.xlsx')
    table_name = 'diagdnhronmorbi'
    # 4. Запись данных в SQL таблицу
    df = pd.read_excel(file_path, sheet_name='Sheet1')

    df.to_sql(
        name=table_name,
        con=engine_single_3,
        if_exists='replace',  # или 'append' для добавления данных
        index=False
    )

    print(f"Данные успешно перенесены в таблицу {table_name}")


# Пример использования
def main():
    file_path = Path('dataset', 'Приказ 168н и 804н 2.0.xls')
    xl = pd.ExcelFile(file_path)
    sheet_names: list[str] = xl.sheet_names
    list_data = [get_data_from_excel(sheet_name, xl) for sheet_name in sheet_names]

    # ----

    final_result = []
    for item in list_data:
        for row in item:
            raw_code_name = row[0]
            raw_code_names = raw_code_name.split(',')
            for raw_code_name in raw_code_names:
                mkb_codes = expand_range(raw_code_name)
                for code_name in mkb_codes:
                    code_name, type_ = code_type(code_name)
                    final_result.append((
                        code_name,
                        type_,
                        *row[1:],
                    ))

    make_df(data=final_result)


if __name__ == '__main__':
    # main()
    to_sql_single_3()  # diagdnhronmorbi