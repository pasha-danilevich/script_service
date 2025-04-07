

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

# --------------------------

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


# ----------------------

agg_df = result_df.groupby('МО').agg({
        'Всего': 'sum',
        'Сельские': 'sum'
    })