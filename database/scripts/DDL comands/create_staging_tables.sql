-- ===================================================================
-- 1. Таблица staging.clients
-- ===================================================================
CREATE TABLE IF NOT EXISTS staging.clients
(
    client_id         INTEGER PRIMARY KEY,
    fio               TEXT NOT NULL,
    passport          TEXT NOT NULL,
    gender            gender_enum NOT NULL,
    birth_date        DATE NOT NULL,
    education         education_enum NOT NULL,
    count_of_children SMALLINT,
    job_type          employment_enum NOT NULL,
    region            TEXT,
    family_status     marital_enum,
    address           TEXT,
    phone             TEXT,
    income            INTEGER NOT NULL,
    file_name         TEXT NOT NULL,
    load_ts           TIMESTAMP NOT NULL,
    CONSTRAINT uq_clients_passport UNIQUE (passport)
);


-- ===================================================================
-- 2. Таблица staging.loans
-- ===================================================================
CREATE TABLE IF NOT EXISTS staging.loans
(
    id              SERIAL PRIMARY KEY,
    client_id       INTEGER   NOT NULL,
    loan_name       TEXT      NOT NULL,
    loan_amount     INTEGER   NOT NULL,
    loan_start_date TIMESTAMP NOT NULL,
    loan_end_date   TIMESTAMP NOT NULL,
    payment_numbers SMALLINT  NOT NULL,
    paid_amount     INTEGER   NOT NULL,
    file_name       TEXT      NOT NULL,
    load_ts         TIMESTAMP NOT NULL,
    CONSTRAINT uq_loans_client UNIQUE (client_id, loan_name),
    FOREIGN KEY (client_id) REFERENCES staging.clients (client_id)
);


-- ===================================================================
-- 3. Таблица staging.payments
-- ===================================================================
CREATE TABLE IF NOT EXISTS staging.payments
(
    id                SERIAL PRIMARY KEY,
    client_id         INTEGER   NOT NULL,
    loan_name         TEXT      NOT NULL,
    payment_number    SMALLINT  NOT NULL,
    payment_date      TIMESTAMP NOT NULL,
    payment_fact_date TIMESTAMP NOT NULL,
    paid_fact_amount  INTEGER   NOT NULL,
    file_name         TEXT      NOT NULL,
    load_ts           TIMESTAMP NOT NULL,
    CONSTRAINT uq_payments_client UNIQUE (client_id, loan_name, payment_number),
    FOREIGN KEY (client_id) REFERENCES staging.clients (client_id),
    FOREIGN KEY (client_id, loan_name) REFERENCES staging.loans (client_id, loan_name)
);
