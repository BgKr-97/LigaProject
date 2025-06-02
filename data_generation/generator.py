import pandas as pd
from faker import Faker
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import random
import string
from typing import Optional, Union, Tuple
from pathlib import Path
import os
fake = Faker('ru_RU')


class DataGenerator:
    def __init__(self, feature_config, output_folder: str):
        self.feature_config = feature_config

        os.makedirs(output_folder, exist_ok=True)
        self.output_folder = output_folder

    @staticmethod
    def maybe_nan(value: Union[int, float, str], fill_prob: float = 0.2) -> Optional[Union[int, float, str]]:
        """Возвращает значение или None с заданной вероятностью."""
        return value if random.random() > fill_prob else None

    @staticmethod
    def generate_loan_count() -> int:
        """Генерирует количество кредитов для клиента по заданным вероятностям."""
        return random.choices([1, 2, 3, 4, 5], weights=[0.7, 0.15, 0.1, 0.04, 0.01], k=1)[0]

    @staticmethod
    def to_json(df: pd.DataFrame, path) -> None:
        """Сохраняет DataFrame в JSON-файл."""
        df.to_json(
            path,
            orient='records',
            force_ascii=False,
            indent=2,
            date_format='iso'
        )

    @staticmethod
    def get_value_by_age(age, age_dict):
        for age_range, value in age_dict.items():
            start, end = map(int, age_range.split('-'))
            if start <= age <= end:
                return age_range, value

    @staticmethod
    def generate_loan_code() -> str:
        """Генерирует код кредита, например 'ABC-12345'."""
        letters = ''.join(random.choices(string.ascii_uppercase, k=3))
        digits = ''.join(random.choices(string.digits, k=5))
        return f"{letters}-{digits}"

    @staticmethod
    def generate_passport_number() -> str:
        """
        Генерирует российский внутренний паспорт в формате:
          XX XX XXXXXX
        где X — случайная цифра.
        Пример: "41 72 177874"
        """
        # Две группы по 2 цифры и одна группа из 6 цифр
        part1 = ''.join(random.choices('0123456789', k=2))
        part2 = ''.join(random.choices('0123456789', k=2))
        part3 = ''.join(random.choices('0123456789', k=6))
        return f"{part1} {part2} {part3}"

    def compute_risk(self, r, d) -> float:
        """
        Вычисляет риск на основе полей клиента r (namedtuple) и feature_config d.
        Суммирует базовый риск 0.1 и добавляет по каждой характеристике.
        """
        risk = 0.1  # базовый
        for field in r._fields:
            value = getattr(r, field)
            if pd.isnull(value):
                risk += d['unknow']
            else:
                if field == 'gender':
                    risk += d['gender']['risk_value'][value]
                elif field == 'birth_date':
                    age = (datetime.today().date() - pd.to_datetime(value).date()).days // 365
                    _, risk_value = self.get_value_by_age(age, d['age']['risk_value'])
                    risk += risk_value
                elif field == 'education':
                    risk += d['education']['risk_value'][value]
                elif field == 'job_type':
                    risk += d['employment_type']['risk_value'][value]
                elif field == 'family_status':
                    risk += d['marital_status']['risk_value'][value]
                elif field == 'count_of_children':
                    risk += d['children_count']['risk_value'][value]
        return min(max(risk, 0), 1)

    def generate_birth_date(self, today: datetime) -> datetime.date:
        """Генерирует дату рождения на основе возрастных групп из feature_config."""
        age_config = self.feature_config['age']
        list_of_ages = age_config['role']
        list_of_p_value = age_config['p_value']

        group = random.choices(list_of_ages, weights=list_of_p_value, k=1)[0]
        days_low, days_high = map(lambda x: int(x) * 365, group.split('-'))
        random_days = random.randint(days_low, days_high)
        birth_date = (today - timedelta(days=random_days)).date()
        return birth_date

    def generate_clients_df(self, num_clients: int) -> pd.DataFrame:
        """
        Генерирует DataFrame с данными о клиентах.

        Колонки: client_id, fio, gender, birth_date, education, count_of_children,
        job_type, region, family_status, address, phone, income.
        """
        today = datetime.today()
        records = []

        for i in range(1, num_clients + 1):
            gender, age, education, children, employment, marital, region = [
                random.choices(k['role'], weights=k['p_value'], k=1)[0]
                for k in self.feature_config.values()
                if isinstance(k, dict)
            ]
            birth_date = self.generate_birth_date(today)
            passport = self.generate_passport_number()

            fio = fake.name()
            address = fake.address()
            phone = fake.phone_number()
            income = random.randint(20000, 300000)

            records.append({
                'client_id': i,
                'fio': fio,
                'passport': passport,
                'gender': gender,
                'birth_date': birth_date.isoformat(),
                'education': education,
                'count_of_children': self.maybe_nan(children),
                'job_type': employment,
                'region': self.maybe_nan(region),
                'family_status': self.maybe_nan(marital),
                'address': self.maybe_nan(address),
                'phone': self.maybe_nan(phone),
                'income': income
            })

        return pd.DataFrame(records)

    def generate_loans_df(self, clients_df: pd.DataFrame, loan_start_date: datetime.date) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Генерирует два DataFrame:
        1) loan_schedule: по каждой записи — параметры кредита (client_id, loan_name, loan_amount, loan_start_date, loan_end_date, payment_numbers)
        2) loan_payments: по каждому платежу каждого кредита с фактами (client_id, loan_name, payment_number, payment_fact_date, paid_amount, loan_start_date)
        """
        loan_schedule = []
        loan_payments = []
        today = datetime.today().date()

        for row in clients_df.itertuples(index=False):
            risk_value = self.compute_risk(row, self.feature_config)

            n_loans = self.generate_loan_count()
            for i in range(n_loans):
                # случайная дата начала кредита между loan_start_date и сегодня
                max_days = (today - loan_start_date).days
                random_offset_days = random.randint(0, max_days)
                random_loan_start = loan_start_date + timedelta(days=random_offset_days)
                loan_end = today

                # количество месяцев
                num_months = ((loan_end.year - random_loan_start.year) * 12 +
                              loan_end.month - random_loan_start.month + 1)

                if num_months <= 0:
                    continue  # пропускаем, если кредит начался сегодня — нет платежей

                loan_amount = random.randint(row.income * 3, row.income * 10)
                monthly_payment = round(loan_amount / num_months)
                loan_name = self.generate_loan_code()

                loan_schedule.append({
                    'client_id': row.client_id,
                    'loan_name': loan_name,
                    'loan_amount': loan_amount,
                    'loan_start_date': random_loan_start.isoformat(),
                    'loan_end_date': loan_end.isoformat(),
                    'payment_numbers': num_months,
                    'paid_amount': monthly_payment
                })

                for month in range(num_months):
                    payment_date = random_loan_start + relativedelta(months=month)
                    actual_payment = monthly_payment
                    payment_fact_date = payment_date - timedelta(days=random.randint(0, 20))

                    if risk_value > 0.3 and random.random() < risk_value:
                        if random.random() < 0.5:
                            actual_payment = monthly_payment - random.randint(1, monthly_payment)
                        else:
                            delay = random.randint(1, 30)
                            payment_fact_date = payment_date + timedelta(days=delay)

                    loan_payments.append({
                        'client_id': row.client_id,
                        'loan_name': loan_name,
                        'payment_number': month + 1,
                        'payment_date': payment_date.isoformat(),
                        'payment_fact_date': payment_fact_date.isoformat(),
                        'paid_fact_amount': actual_payment,
                        'loan_start_date': random_loan_start.isoformat()
                    })

        sort_schedule = pd.DataFrame(loan_schedule).sort_values('loan_start_date')
        sort_payments = (
            pd.DataFrame(loan_payments)
            .sort_values(['loan_start_date', 'payment_number'])
            .drop('loan_start_date', axis=1)
        )

        return sort_schedule, sort_payments

    def generate_data(self, num_clients: int, loan_start_date: str) -> None:
        """
        Основной метод генерации данных:
        1) генерирует клиентов и сохраняет в JSON
        2) генерирует кредиты и платежи и сохраняет тоже в JSON
        Принимает именованные аргументы:
          clients_file, loans_file, payments_file — пути до JSON-файлов.
        """
        # Преобразование строк в даты
        start_date = datetime.strptime(loan_start_date, "%Y-%m-%d").date()

        clients_path = f"{self.output_folder}/clients.json"
        loans_path = f"{self.output_folder}/loans.json"
        payments_path = f"{self.output_folder}/payments.json"

        # Генерация клиентов
        clients_df = self.generate_clients_df(num_clients)
        clients_df.to_json(clients_path, orient='records', indent=2, force_ascii=False)

        # Генерация займов и платежей
        loan_schedule_df, loan_payments_df = self.generate_loans_df(clients_df, start_date)
        loan_schedule_df.to_json(loans_path, orient='records', indent=2, force_ascii=False)
        loan_payments_df.to_json(payments_path, orient='records', indent=2, force_ascii=False)


    @staticmethod
    def split_jsons_by_loan_start_date(raw_files_folder: str, output_dir: str, parts: int = 5):
        """
        Разбивает три JSON-файла (clients.json, loans.json, payments.json)
        из папки raw_files_folder на N кумулятивных частей по дате старта кредита.

        В output_dir будут созданы подпапки:
          parts/part_1, parts/part_2, …, parts/part_N
        и внутри каждой — три JSON: clients_i.json, loans_i.json, payments_i.json.
        """
        clients_path = f"{raw_files_folder}/clients.json"
        loans_path = f"{raw_files_folder}/loans.json"
        payments_path = f"{raw_files_folder}/payments.json"

        clients_df = pd.read_json(clients_path)
        loans_df = pd.read_json(loans_path)
        payments_df = pd.read_json(payments_path)

        # сортируем кредиты по дате начала
        loans_sorted = loans_df.sort_values('loan_start_date').reset_index(drop=True)
        total_loans = len(loans_sorted)
        rows_per_part = total_loans // parts + 1

        for i in range(1, parts + 1):
            part_loans = loans_sorted.iloc[: i * rows_per_part].copy()

            # сохраняем кредиты
            part_loans_folder = Path(output_dir) / f"part_{i}"
            part_loans_folder.mkdir(parents=True, exist_ok=True)
            part_loans.to_json(
                part_loans_folder / f"loans_{i}.json",
                orient='records', indent=2, force_ascii=False
            )

            # платежи для этих кредитов
            part_keys = part_loans[['client_id', 'loan_name']]
            part_payments = payments_df.merge(part_keys, on=['client_id', 'loan_name'], how='inner')
            part_payments.to_json(
                part_loans_folder / f"payments_{i}.json",
                orient='records', indent=2, force_ascii=False
            )

            # клиенты, участвующие в этой части
            client_ids = part_loans['client_id'].unique()
            part_clients = clients_df[clients_df['client_id'].isin(client_ids)]
            part_clients.to_json(
                part_loans_folder / f"clients_{i}.json",
                orient='records', indent=2, force_ascii=False
            )
