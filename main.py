import pandas as pd
import requests
import os
import urllib.parse
from sqlalchemy import create_engine

DB_USER = 'postgres'
DB_PASSWORD = 'password'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'sirius_db'
TABLE_NAME = 'student_tasks'

DATASET_PATH = 'data.csv'
DOWNLOAD_DIR = 'downloads'

def get_db_engine():
    connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(connection_string)

def process_and_save_to_db(filepath, engine):
    print(f"Чтение датасета: {filepath}...")
    
    if filepath.endswith('.xlsx') or filepath.endswith('.xls'):
        df = pd.read_excel(filepath)
    else:
        df = pd.read_csv(filepath)

    df = df.replace({'NULL': None, '#N/A': None})

    df.columns = df.columns.str.strip()

    column_mapping = {
        'session_id': 'session_id',
        'id': 'record_id',
        '№ задания': 'task_number',
        'Балл': 'score',
        'Ответ': 'answer_json',
        'Ответ (файлы)': 'file_url',
        'Фамилия': 'surname',
        'Имя': 'name',
        'Отчество': 'patronymic'
    }
    
    existing_columns = [col for col in column_mapping.keys() if col in df.columns]
    df = df[existing_columns].rename(columns=column_mapping)

    print(f"Сохранение {len(df)} строк в PostgreSQL (таблица: {TABLE_NAME})...")
    df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
    print("Данные успешно сохранены в БД!")
    return df

def download_files(df):
    if 'file_url' not in df.columns:
        print("Колонка 'file_url' не найдена. Пропускаем скачивание.")
        return

    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
        print(f"Создана папка: {DOWNLOAD_DIR}")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    download_count = 0
    for _, row in df.iterrows():
        url = row['file_url']
        
        if pd.notna(url) and isinstance(url, str) and url.startswith('http'):
            try:
                parsed_url = urllib.parse.urlparse(url)
                original_filename = os.path.basename(parsed_url.path) or "file.bin"
                
                record_id = row.get('record_id', 'no_id')
                safe_filename = f"{record_id}_{original_filename}"
                filepath = os.path.join(DOWNLOAD_DIR, safe_filename)

                response = requests.get(url, stream=True, headers=headers, timeout=10)
                if response.status_code == 200:
                    with open(filepath, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    print(f"[OK] {safe_filename}")
                    download_count += 1
                else:
                    print(f"[FAIL] Код {response.status_code}: {url}")
            except Exception as e:
                print(f"[ERROR] {url}: {e}")

    print(f"Скачивание завершено. Всего файлов: {download_count}")

def main():
    try:
        engine = get_db_engine()
        if not os.path.exists(DATASET_PATH):
            print(f"Файл {DATASET_PATH} не найден!")
            return
            
        df = process_and_save_to_db(DATASET_PATH, engine)
        download_files(df)
        print("Программа успешно завершила работу!")
    except Exception as e:
        print(f"Критическая ошибка: {e}")

if __name__ == "__main__":
    main()