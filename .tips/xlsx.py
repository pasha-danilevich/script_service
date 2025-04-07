from pathlib import Path

import pandas as pd


def get_df_from_xlsx(path: Path, skip_rows: int = 0) -> pd.DataFrame:
    xl = pd.ExcelFile(path)

    df_ = pd.read_excel(
        xl,
        sheet_name=xl.sheet_names[0],
        skiprows=skip_rows,
    )

    return df_


# ----------------

def save_xlsx(df: pd.DataFrame, path_to: str, name: str = 'result'):
    with pd.ExcelWriter(Path(path_to + f'{name}.xlsx')) as writer:
        df.to_excel(writer, index=False, sheet_name='A')
        writer.sheets['A'].autofit()
        writer.sheets['A'].autofilter(0, 0, df.shape[0], df.shape[1])

# ----------------

with pd.ExcelWriter(f'result.xlsx') as writer:
    result_df.to_excel(writer, sheet_name='Результат', index=False)
    agg_df.to_excel(writer, sheet_name='Агрегация', index=True)

    writer.sheets['Результат'].autofit()
    writer.sheets['Результат'].autofilter(0, 0, result_df.shape[0], result_df.shape[1])