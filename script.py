import os
import sys
import pandas as pd
from datetime import datetime, timedelta

def aggregate_logs(date):
    input_dir = 'input'
    output_dir = 'output'
    
    os.makedirs(output_dir, exist_ok=True)
    
    start_date = datetime.strptime(date, '%Y-%m-%d') - timedelta(days=7)
    end_date = datetime.strptime(date, '%Y-%m-%d')
    
    all_data = []
    for i in range(7):
        current_date = start_date + timedelta(days=i)
        file_name = f"{current_date.strftime('%Y-%m-%d')}.csv"
        file_path = os.path.join(input_dir, file_name)
        
        if os.path.exists(file_path):
            data = pd.read_csv(file_path, names=['email', 'action', 'dt'])
            all_data.append(data)

    # Объединяем данные из всех файлов
    all_data = pd.concat(all_data, ignore_index=True)

    # Агрегируем данные
    aggregated_data = all_data.groupby(['email', 'action']).size().unstack(fill_value=0)
    aggregated_data = aggregated_data.reset_index()
    aggregated_data = aggregated_data.reindex(columns=['email', 'CREATE', 'READ', 'UPDATE', 'DELETE'], fill_value=0)

    # Записываем результат
    output_file_path = os.path.join(output_dir, f"{end_date.strftime('%Y-%m-%d')}.csv")
    aggregated_data.to_csv(output_file_path, index=False)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <YYYY-mm-dd>")
        sys.exit(1)
    
    date = sys.argv[1]
    aggregate_logs(date)