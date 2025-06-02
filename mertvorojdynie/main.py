from pathlib import Path

import pandas as pd

from mertvorojdynie.create_region import create_redion_from_addrerss_single

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

def get_df_from_excel(sheet_name: str, xl: pd.ExcelFile) -> pd.DataFrame:
    df = pd.read_excel(
        xl,
        sheet_name=sheet_name,
        skiprows=0,
    )
    columns = ['Номер свидетельства', 'Адрес', 'Период смерти']
    return df[columns]

def save_xlsx(df: pd.DataFrame, name: str = 'result'):
    with pd.ExcelWriter(Path(f'{name}.xlsx')) as writer:
        df.to_excel(writer, index=False, sheet_name='A')
        writer.sheets['A'].autofit()
        writer.sheets['A'].autofilter(0, 0, df.shape[0], df.shape[1])

def main():
    file_path = Path('dataset', 'мертворождения.xlsx')
    xl = pd.ExcelFile(file_path)
    df = get_df_from_excel('Report', xl)
    df = df[df['Период смерти'] == 'Мертворожденный']
    df = create_redion_from_addrerss_single(df, ['Адрес'])
    # print(df[10:30])
    # Группируем по 'Регион' и считаем количество 'Номер свидетельства'
    count_by_region = df.groupby('Регион').size().reset_index(name='Количество мертворожденный')
    # print(count_by_region)

    save_xlsx(df=count_by_region, name='Кол-во мертворожденных по регионам')



if __name__ == '__main__':
    main()