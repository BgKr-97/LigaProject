-- ===================================================================
-- 1. Таблица core.clients (копия staging.clients без file_name/load_ts)
-- ===================================================================
CREATE TABLE IF NOT EXISTS core.clients
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
    income            INTEGER NOT NULL
);


-- ===================================================================
-- 2. Таблица core.loans (копия staging.loans без file_name/load_ts)
-- ===================================================================
CREATE TABLE IF NOT EXISTS core.loans
(
    id              SERIAL PRIMARY KEY,
    client_id       INTEGER   NOT NULL,
    loan_name       TEXT      NOT NULL,
    loan_amount     INTEGER   NOT NULL,
    loan_start_date TIMESTAMP NOT NULL,
    loan_end_date   TIMESTAMP NOT NULL,
    payment_numbers SMALLINT  NOT NULL,
    paid_amount     INTEGER   NOT NULL,
    FOREIGN KEY (client_id) REFERENCES core.clients (client_id),
    CONSTRAINT uq_core_loans_client UNIQUE (client_id, loan_name)
);


-- ===================================================================
-- 3. Таблица core.payments (копия staging.payments + поле status)
-- ===================================================================
CREATE TABLE IF NOT EXISTS core.payments
(
    id                SERIAL PRIMARY KEY,
    client_id         INTEGER   NOT NULL,
    loan_name         TEXT      NOT NULL,
    payment_number    SMALLINT  NOT NULL,
    payment_date      TIMESTAMP NOT NULL,
    payment_fact_date TIMESTAMP NOT NULL,
    paid_fact_amount  INTEGER   NOT NULL,
    status            BOOLEAN   NOT NULL,
    FOREIGN KEY (client_id) REFERENCES core.clients (client_id),
    FOREIGN KEY (client_id, loan_name) REFERENCES core.loans (client_id, loan_name),
    CONSTRAINT uq_core_payments_client UNIQUE (client_id, loan_name, payment_number)
);