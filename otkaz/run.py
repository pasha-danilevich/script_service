from pathlib import Path
import pandas as pd
from sqlalchemy import text

from settings import engine, BASE_PATH


def get_db_from_db(_engine, path: Path, condition: dict[str, int | str | tuple] = None) -> pd.DataFrame:
    with open(path, 'r') as f:
        sql_text = f.read()

    if isinstance(sql_text, str) and '{' in sql_text and condition is not None:
        # Обработка кортежей в condition
        for key, value in condition.items():
            if isinstance(value, tuple):
                if len(value) == 1:
                    # Если кортеж из одного элемента, преобразуем его в строку
                    condition[key] = f"('{value[0]}')"
                else:
                    # Если кортеж из нескольких элементов, оставляем как есть
                    condition[key] = str(value)
        sql_text = sql_text.format(**condition)

    try:
        with _engine.connect() as con:
            df = pd.read_sql_query(text(sql_text), con=con)
            return df
    except Exception as e:
        # Запись ошибки в файл
        with open('error_log.txt', 'a') as f:
            f.write(f"Ошибка при выполнении запроса: {e}\n")


def get_df_from_xlsx(path, skip_rows: int, sheet_index: int = 0) -> pd.DataFrame:
    xl = pd.ExcelFile(path)

    _df = pd.read_excel(
        xl,
        skiprows=skip_rows,
    )

    return _df




def main():
    excel_file_name = 'отказ 2023-2024 + рецепты.xlsx'
    df_2023 = get_df_from_xlsx(Path(BASE_PATH) / 'otkaz' / 'dataset' / excel_file_name, skip_rows=0,
                               sheet_index=0, )

    df_2023['custom_id'] = (
        df_2023['FAM'].fillna('').str.cat(df_2023['IM'].fillna(''), sep='')  # Объединение FAM и IM
        .str.cat(df_2023['OT'].fillna(''), sep='')  # Добавление OT
        .str.cat(df_2023['RDAT'].fillna(''), sep='')  # Добавление RDAT
        .str.replace('/', '-')
        .str.replace("'", "")
    )


    print(df_2023.head())

    # -----


    df_2024 = get_df_from_xlsx(Path(BASE_PATH) / 'otkaz' / 'dataset' / excel_file_name, skip_rows=0,
                                   sheet_index=1, )

    df_2024['custom_id'] = (
        df_2024['FAM'].fillna('').str.cat(df_2024['IM'].fillna(''), sep='')  # Объединение FAM и IM
        .str.cat(df_2024['OT'].fillna(''), sep='')  # Добавление OT
        .str.cat(df_2024['RDAT'].fillna(''), sep='')  # Добавление RDAT
        .str.replace('/', '-')
        .str.replace("'", "")
    )


    print(df_2024.head())

    # -----

    combined_list = df_2023['custom_id'].tolist() + df_2024['custom_id'].tolist()
    ids = tuple(set(combined_list))

    # -----
    print(ids[:10])

    sql_path = Path(BASE_PATH) / 'otkaz' / 'sql.sql'
    df_db = get_db_from_db(engine, sql_path, dict(ids=ids))
    print(df_db.head())

    df_2023_2024 = pd.concat([df_2024, df_2023])

    result_df = pd.merge(
        df_2023_2024,  # Левый DataFrame
        df_db,  # Правый DataFrame
        how='left',  # Тип объединения: 'inner', 'outer', 'left', 'right'
        on='custom_id',  # Столбец или список столбцов для объединения
        left_on=None,  # Столбец(ы) в левом DataFrame для объединения
        right_on=None,  # Столбец(ы) в правом DataFrame для объединения
        left_index=False,  # Использовать индекс левого DataFrame как ключ
        right_index=False,  # Использовать индекс правого DataFrame как ключ
    )

    result_df = get_df_from_xlsx(Path(BASE_PATH) / 'otkaz' / excel_file_name, skip_rows=0,
                                                              sheet_index=1, )

    df_count = pd.DataFrame({
        'Всего людей': [result_df['custom_id'].nunique()],
        'Всего людей с рецептами': [result_df["U"].nunique()],
        'Количество рецептов': [result_df["Количество рецептов"].sum()],
        'Количество упаковок': [result_df["Количество упаковок"].sum()],
        'Сумма возмещения': [result_df["Сумма возмещения"].sum()],
    })

    with pd.ExcelWriter(f'result.xlsx') as writer:
        result_df.to_excel(writer, sheet_name='Результат', index=False)

        writer.sheets['Результат'].autofit()
        writer.sheets['Результат'].autofilter(0, 0, result_df.shape[0], result_df.shape[1])

    with pd.ExcelWriter(f'result1.xlsx') as writer:
        df_count.to_excel(writer, sheet_name='Итого', index=False)




if __name__ == "__main__":
    main()