-- Переносим клиентов из staging.clients в core.clients.
-- При конфликте по client_id обновляем все остальные поля.

INSERT INTO core.clients
(
    client_id,
    fio,
    passport,
    gender,
    birth_date,
    education,
    count_of_children,
    job_type,
    region,
    family_status,
    address,
    phone,
    income
)
SELECT
    s.client_id,
    s.fio,
    s.passport,
    s.gender,
    s.birth_date,
    s.education,
    s.count_of_children,
    s.job_type,
    s.region,
    s.family_status,
    s.address,
    s.phone,
    s.income
FROM staging.clients AS s
ON CONFLICT (client_id) DO UPDATE
  SET
    fio               = EXCLUDED.fio,
    passport          = EXCLUDED.passport,
    gender            = EXCLUDED.gender,
    birth_date        = EXCLUDED.birth_date,
    education         = EXCLUDED.education,
    count_of_children = EXCLUDED.count_of_children,
    job_type          = EXCLUDED.job_type,
    region            = EXCLUDED.region,
    family_status     = EXCLUDED.family_status,
    address           = EXCLUDED.address,
    phone             = EXCLUDED.phone,
    income            = EXCLUDED.income;
