INSERT INTO staging.clients
(
    client_id,
    passport,
    fio,
    gender,
    birth_date,
    education,
    count_of_children,
    job_type,
    region,
    family_status,
    address,
    phone,
    income,
    file_name,
    load_ts
)
SELECT
    t.client_id,
    t.passport,
    t.fio,
    t.gender,
    t.birth_date,
    t.education,
    t.count_of_children,
    t.job_type,
    t.region,
    t.family_status,
    t.address,
    t.phone,
    t.income,
    t.file_name,
    t.load_ts
FROM temp_clients AS t
ON CONFLICT (passport) DO UPDATE
  SET
    client_id         = EXCLUDED.client_id,
    fio               = EXCLUDED.fio,
    gender            = EXCLUDED.gender,
    birth_date        = EXCLUDED.birth_date,
    education         = EXCLUDED.education,
    count_of_children = EXCLUDED.count_of_children,
    job_type          = EXCLUDED.job_type,
    region            = EXCLUDED.region,
    family_status     = EXCLUDED.family_status,
    address           = EXCLUDED.address,
    phone             = EXCLUDED.phone,
    income            = EXCLUDED.income,
    file_name         = EXCLUDED.file_name,
    load_ts           = EXCLUDED.load_ts;
