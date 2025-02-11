import os
from pathlib import Path
import pandas as pd
from settings import BASE_PATH
from utils import ExcelHandler


current_file_path = Path(__file__).resolve()
dir_name = current_file_path.parent.name
REPORT_FILE_NAME = 'Отчет_о_наполняемости_блока_Медицинское_оборудование_11022025.xlsx'

def main():
    eh = ExcelHandler()

    # hardware_code_df
    hc_df = eh.read_excel(
        file_path=os.path.join(BASE_PATH, dir_name, 'dataset', 'Коды цифрового оборудования.xlsx'),
        skiprows=1
    )
    print(hc_df.head())

    # content_report
    cr_df = eh.read_excel(
        file_path=os.path.join(BASE_PATH, dir_name, 'dataset', REPORT_FILE_NAME),
        skiprows=4,
        nrows=1000
    )
    merge_field_name: str = 'Код типа медицинского изделия'
    # Разделяем строку и берем первый элемент
    cr_df[merge_field_name] = cr_df['Тип медицинского изделия'].str.split(' ', n=1).str[0]
    cr_df[merge_field_name] = pd.to_numeric(cr_df[merge_field_name], errors='coerce')

    print(cr_df.head())

    result_df = pd.merge(
        hc_df,  # Левый DataFrame
        cr_df,  # Правый DataFrame
        how='inner',  # Тип объединения: 'inner', 'outer', 'left', 'right'
        on=merge_field_name,  # Столбец или список столбцов для объединения
        left_on=None,  # Столбец(ы) в левом DataFrame для объединения
        right_on=None,  # Столбец(ы) в правом DataFrame для объединения
        left_index=False,  # Использовать индекс левого DataFrame как ключ
        right_index=False,  # Использовать индекс правого DataFrame как ключ
        suffixes=('_hc_df', '_cr_df')  # Суффиксы для столбцов с одинаковыми именами
    )

    eh.write_excel(result_df, dir_name=dir_name, file_name='Коды цифрового оборудования входящие в отчет.xlsx')

    print(result_df.head())


if __name__ == '__main__':
    main()