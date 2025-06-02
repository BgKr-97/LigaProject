import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


class DBExtractor:
    _connected_once = False

    def __init__(self, dbname, user, password, host, port, verbose: bool = True):
        """
        Инициализирует подключение к PostgreSQL.
        """
        conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        try:
            self.engine = create_engine(conn_str)
            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1"))

            if verbose and not DBExtractor._connected_once:
                print(f"✅  Успешное подключение к БД {dbname}!")
                DBExtractor._connected_once = True
        except SQLAlchemyError as e:
            print(f"❌ Ошибка при подключении к базе данных: {e}")
            raise

    def _read_sql(self, path: str) -> str:
        """
        Считывает SQL из файла по полному пути path.
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"SQL-файл не найден: {path}")
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            print(f"❌ Ошибка при чтении SQL-файла {path}: {e}")
            raise

    def execute_sql_script(self, sql_file_path: str):
        """
        Считывает и выполняет SQL-скрипт целиком.
        """
        try:
            sql_text = self._read_sql(sql_file_path)
            with self.engine.begin() as conn:
                conn.execute(text(sql_text))
        except SQLAlchemyError as e:
            print(f"❌ Ошибка при выполнении SQL-скрипта {sql_file_path}: {e}")
            raise

    def incremental_load(self,
                         df: pd.DataFrame,
                         create_temp_sql_path: str,
                         insert_sql_path: str,
                         temp_table_name: str):
        """
        Инкрементальная загрузка данных во временную таблицу и далее в целевую.
        """
        try:
            sql_create_temp = self._read_sql(create_temp_sql_path)
            sql_insert = self._read_sql(insert_sql_path)

            with self.engine.begin() as conn:
                conn.execute(text(sql_create_temp))
                df.to_sql(
                    name=temp_table_name,
                    con=conn,
                    index=False,
                    if_exists="append",
                    method="multi"
                )
                conn.execute(text(sql_insert))
        except FileNotFoundError as e:
            print(f"❌ Ошибка загрузки: файл не найден — {e}")
            raise
        except SQLAlchemyError as e:
            print(f"❌ Ошибка при выполнении загрузки данных: {e}")
            raise
        except Exception as e:
            print(f"❌ Непредвиденная ошибка: {e}")
            raise
