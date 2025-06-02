DROP TABLE IF EXISTS temp_clients;
CREATE TEMPORARY TABLE temp_clients
(
    client_id         INTEGER,
    passport          TEXT NOT NULL,
    fio               TEXT NOT NULL,
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
    load_ts           TIMESTAMP NOT NULL
);
