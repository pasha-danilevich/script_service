

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

    with _engine.connect() as con:
        print(sql_text)  # Для отладки
        df = pd.read_sql_query(text(sql_text), con=con)
        return df