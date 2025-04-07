from pathlib import Path

import pandas as pd

from settings import BASE_PATH

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


def get_df_from_xlsx(path: Path, skip_rows: int = 0) -> pd.DataFrame:
    xl = pd.ExcelFile(path)

    df_ = pd.read_excel(
        xl,
        sheet_name=xl.sheet_names[0],
        skiprows=skip_rows,
    )

    return df_


def save_xlsx(df: pd.DataFrame, name: str = 'result'):
    with pd.ExcelWriter(f'{name}.xlsx') as writer:
        df.to_excel(writer, index=False, sheet_name='A')
        writer.sheets['A'].autofit()
        writer.sheets['A'].autofilter(0, 0, df.shape[0], df.shape[1])


def main():
    path = Path(BASE_PATH / 'posesheniy' / 'dataset' / 'посещения.xlsx')
    df = get_df_from_xlsx(path, skip_rows=17)

    result_dict = {
        'МО': [],
        'СП': [],
        'Всего': [],
        'Сельские': [],
    }

    mo = ''
    for index, row in df.iterrows():
        row_0 = str(row.iloc[0]).strip().upper()
        if row_0.startswith('ГАУЗ') or row_0.startswith('ГБУЗ'):
            mo = row.iloc[0]
        if row_0.startswith('ИТОГО'):
            if ('ЖЕНСК' in row_0 and 'КОН' in row_0) or ('ЖК' in row_0) and not ('ЛУЖКИ' in row_0):
                result_dict['МО'].append(mo)
                result_dict['СП'].append(row.iloc[0])
                result_dict['Всего'].append(int(row.iloc[2]))
                result_dict['Сельские'].append(int(row.iloc[3]))

    result_df = pd.DataFrame(result_dict)
    agg_df = result_df.groupby('МО').agg({
        'Всего': 'sum',
        'Сельские': 'sum'
    })
    print(agg_df)

    with pd.ExcelWriter(f'Женские консультации АП обращения по 28.02.2025.xlsx') as writer:
        result_df.to_excel(writer, sheet_name='Результат', index=False)
        agg_df.to_excel(writer, sheet_name='Агрегация', index=True)


if __name__ == '__main__':
    main()