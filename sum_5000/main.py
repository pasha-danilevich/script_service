import os
import pandas as pd

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

stt = {
    'skiprows': 8,
    'header': 0
}

def main():
    
    folder_path = 'datasets'
    files = [f for f in os.listdir(folder_path) if f.endswith('.xlsx')]
    dfs = [pd.read_excel(os.path.join(folder_path, file), **stt) for file in files]
    

    numeric_cols = ['4', '5', '6', '7', '8', '9', '10', '11', '12', '13']
    df_sum = dfs[0].copy()
    df_sum[numeric_cols] = sum([df[numeric_cols] for df in dfs])
    
    # Сохраняем в новый Excel (с заголовком, как в исходном файле)
    with pd.ExcelWriter('result.xlsx', engine='openpyxl') as writer:
        # Записываем заголовок (первые 9 строк можно скопировать из исходного файла)
        pd.DataFrame().to_excel(writer, sheet_name='Sheet1', index=False)
        df_sum.to_excel(writer, sheet_name='Sheet1', startrow=9, index=False)
    
    print("Файлы успешно суммированы и сохранены в result.xlsx!")

if __name__ == '__main__':
    main()
