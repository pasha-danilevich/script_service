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
    columns = ['ФИО', 'МО учета', 'Срок', 'МО Родоразрешения']
    return df[columns]

def save_xlsx(df: pd.DataFrame, name: str = 'result'):
    with pd.ExcelWriter(Path(f'{name}.xlsx')) as writer:
        df.to_excel(writer, index=False, sheet_name='A')
        writer.sheets['A'].autofit()
        writer.sheets['A'].autofilter(0, 0, df.shape[0], df.shape[1])

def main():
    file_path = Path('dataset', 'Регистр беременных.xlsx')
    xl = pd.ExcelFile(file_path)
    df = get_df_from_excel('Лист1', xl)

    df = df.dropna(subset=['МО Родоразрешения'])
    # df = df[df['МО Родоразрешения'] == 'ГБУЗ РК "РКБ им.Н.А.Семашко"']
    # df = df[(df['Срок'] >= 28) & (df['Срок'] <= 36)] # между 28 и 36
    count_by_mo = df.groupby('Срок').size().reset_index(name='Количество родивших')
    # count_by_mo = df.groupby('МО учета', as_index=False)['ФИО'].nunique()
    print(df.head(25))
    print('==' * 100)
    print(count_by_mo[:50])
    save_xlsx(df=count_by_mo, name='Количество родивших')



if __name__ == '__main__':
    main()