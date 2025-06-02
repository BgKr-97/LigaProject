import os
import sys
import argparse
import json
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

from data_generation.generator import DataGenerator
from data_generation.config import FEATURE_CONFIG
from database.db_extractor import DBExtractor


def json_to_dataframe(path: str) -> pd.DataFrame:
    """
    Преобразует JSON-файл в pandas.DataFrame с добавлением полей:
    -- file_name: имя файла (basename),
    -- load_ts: текущий timestamp.

    Параметры:
    -- path: путь до JSON-файла
    """
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    df = pd.DataFrame(data)
    df['file_name'] = os.path.basename(path)
    df['load_ts'] = datetime.today()

    return df


def cmd_generate(args, raw_files_folder: str, start_loan_date: str) -> None:
    """
    Режим generate: пересоздаём полностью «сырые» JSON: clients.json, loans.json, payments.json.
    """
    generator = DataGenerator(FEATURE_CONFIG, raw_files_folder)

    generator.generate_data(args.num_clients, start_loan_date)
    print('✅  «Сырые» JSON-файлы успешно сгенерированы.\n')


def cmd_split(args, raw_files_folder: str, parts_files_folder: str) -> None:
    """
    Режим split: разбиваем существующие JSON на кумулятивные части по дате старта кредита.
    """
    existing_jsons = [f for f in os.listdir(raw_files_folder) if f.endswith(".json")]
    if len(existing_jsons) != 3:
        print("❌  ОШИБКА: в папке raw отсутствуют все три JSON (clients.json, loans.json, payments.json).")
        print("Сначала выполните: python main.py generate")
        sys.exit(1)

    os.makedirs(parts_files_folder, exist_ok=True)

    generator = DataGenerator(FEATURE_CONFIG, raw_files_folder)
    generator.split_jsons_by_loan_start_date(
        raw_files_folder=raw_files_folder,
        output_dir=parts_files_folder,
        parts=args.parts
    )
    print(f'✅  «Сырые» данные разбиты на {args.parts} частей в папке "{parts_files_folder}".\n')


def cmd_shema(extractor: DBExtractor) -> None:
    """
    Режим shema: создаём схемы staging и core + необходимые таблицы и типы.
    """
    extractor.execute_sql_script("database/scripts/DDL comands/create_shemas.sql")
    extractor.execute_sql_script("database/scripts/DDL comands/create_staging_tables.sql")
    extractor.execute_sql_script("database/scripts/DDL comands/create_core_tables.sql")
    extractor.execute_sql_script("database/scripts/DDL comands/create_datamart_table.sql")
    print("✅  Схемы и таблицы успешно созданы.\n")


def cmd_load(part_num: int, parts_files_folder: str, extractor: DBExtractor) -> None:
    """
    Режим load: инкрементально загружаем в staging и core одну указанную часть part_num.
    Проверяем, что нельзя пропустить части (на основе staging.clients.file_name).
    """
    if part_num < 1:
        print("❌  ОШИБКА: номер части должен быть >= 1.")
        sys.exit(1)

    part_folder = f"{parts_files_folder}/part_{part_num}"
    if not os.path.isdir(part_folder):
        print(f"❌  ОШИБКА: папки '{part_folder}' не существует. Сначала выполните 'split'.")
        sys.exit(1)

    # Проверяем наличие JSON-файлов внутри part_N
    clients_path  = f"{part_folder}/clients_{part_num}.json"
    loans_path    = f"{part_folder}/loans_{part_num}.json"
    payments_path = f"{part_folder}/payments_{part_num}.json"

    for p in (clients_path, loans_path, payments_path):
        if not os.path.exists(p):
            print(f"❌  ОШИБКА: не найден файл '{p}'. Невозможно загрузить часть {part_num}.")
            sys.exit(1)

    # 0) Узнаём, какая часть уже загружена (по file_name в staging.clients)
    with extractor.engine.connect() as conn:
        result = conn.execute(text(
            """
            SELECT
              MAX((regexp_match(file_name, 'clients_(\\d+)\\.json'))[1]::INTEGER) AS last_part
            FROM staging.clients;
            """
        ))
        last_part = result.scalar()  # None, если ещё не было записей

    # 1) Проверка последовательности
    if last_part is None:
        if part_num != 1:
            print("❌  ОШИБКА: ещё не загружена ни одна часть. Сначала выполните загрузку part_1.")
            sys.exit(1)
    else:
        if part_num != last_part + 1:
            print(f"❌ ОШИБКА: последняя загруженная часть — part_{last_part}. "
                  f"Теперь можно загружать только part_{last_part + 1}.")
            sys.exit(1)

    # 2) Инкрементальная загрузка клиентов
    df_clients = json_to_dataframe(clients_path)
    extractor.incremental_load(
        df=df_clients,
        create_temp_sql_path="database/scripts/DDL comands/create_temp_table_clients.sql",
        insert_sql_path="database/scripts/DML comands/upsert_clients.sql",
        temp_table_name="temp_clients"
    )
    extractor.execute_sql_script("database/scripts/DML comands/insert_to_clients.sql")
    print(f"✅  Успешная загрузка clients из part_{part_num} -> staging.clients -> core.clients.")

    # 3) Инкрементальная загрузка займов
    df_loans = json_to_dataframe(loans_path)
    extractor.incremental_load(
        df=df_loans,
        create_temp_sql_path="database/scripts/DDL comands/create_temp_table_loans.sql",
        insert_sql_path="database/scripts/dml comands/upsert_loans.sql",
        temp_table_name="temp_loans"
    )
    extractor.execute_sql_script("database/scripts/DML comands/insert_to_loans.sql")
    print(f"✅  Успешная загрузка loans из part_{part_num} -> staging.loans -> core.loans.")

    # 4) Инкрементальная загрузка платежей
    df_payments = json_to_dataframe(payments_path)
    extractor.incremental_load(
        df=df_payments,
        create_temp_sql_path="database/scripts/DDL comands/create_temp_table_payments.sql",
        insert_sql_path="database/scripts/DML comands/upsert_payments.sql",
        temp_table_name="temp_payments"
    )
    extractor.execute_sql_script("database/scripts/DML comands/insert_to_payments.sql")
    print(f"✅  Успешная загрузка payments из part_{part_num} -> staging.payments -> core.payments.\n")

    extractor.execute_sql_script("database/scripts/DML comands/insert_to_mart.sql")
    print(f"✅  Успешная загрузка clients, loans, payments из part_{part_num} -> staging.payments -> core.payments -> mart.data_mart.\n")


def main():
    load_dotenv()
    # Параметры подключения к БД
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASS = os.getenv("DB_PASS")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = int(os.getenv("DB_PORT"))

    # Папки с данными
    RAW_DIR = os.getenv("RAW_DIR")
    SPLIT_DIR = os.getenv("SPLIT_DIR")

    # Настройки генерации данных
    START_LOAN_DATE=os.getenv("START_LOAN_DATE")

    parser = argparse.ArgumentParser(
        description="CLI для генерации, разбиения и инкрементальной загрузки данных"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- Подкоманда generate ---
    gen_parser = subparsers.add_parser("generate", help="Сгенерировать новые «сырые» JSON-файлы")
    gen_parser.add_argument(
        "--num-clients", type=int, default=20,
        help="Сколько клиентов генерировать (по умолчанию 20)"
    )

    # --- Подкоманда split ---
    split_parser = subparsers.add_parser("split", help="Разбить существующие JSON на кумулятивные части")
    split_parser.add_argument(
        "--parts", type=int, default=5,
        help="На сколько частей разбивать (по умолчанию 5)"
    )

    # --- Подкоманда shema ---
    subparsers.add_parser("shema", help="Создать схемы и таблицы в БД (once)")

    # --- Подкоманда load ---
    load_parser = subparsers.add_parser(
        "load",
        help="Инкрементально загрузить одну часть в staging и core"
    )
    group = load_parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--part", type=int,
        help="Номер части для загрузки (part_N)"
    )
    group.add_argument(
        "--all", action="store_true",
        help="Загрузить все доступные части последовательно"
    )

    args = parser.parse_args()

    if args.command == "generate":
        cmd_generate(args, RAW_DIR, START_LOAN_DATE)

    elif args.command == "split":
        cmd_split(args, RAW_DIR, SPLIT_DIR)

    elif args.command == "shema":
        extractor = DBExtractor(
            dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
        )
        cmd_shema(extractor)

    elif args.command == "load":
        extractor = DBExtractor(
            dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
        )

        if args.all:
            # Загружаем все папки part_1, part_2, … последовательно
            subdirs = sorted([d for d in os.listdir(SPLIT_DIR)], key=lambda x: int(x.split("_")[1]))

            for d in subdirs:
                part_num = int(d.split("_")[1])
                print(f"▶️  Загружаем {d} …")
                cmd_load(part_num, SPLIT_DIR, extractor)
        else:
            # Загрузка конкретной части
            cmd_load(args.part, SPLIT_DIR, extractor)

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
