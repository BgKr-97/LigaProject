INSERT INTO mart.data_mart (
    client_id, fio, passport, gender, birth_date, education, count_of_children, job_type,
    region, family_status, income, loan_name, loan_amount, loan_start_date, loan_end_date,
    paid_amount, payment_number, payment_date, payment_fact_date, paid_fact_amount, status
)
SELECT
    c.client_id,
    c.fio,
    c.passport,
    c.gender::text,
    c.birth_date,
    c.education::text,
    c.count_of_children,
    c.job_type::text,
    c.region,
    c.family_status::text,
    c.income,
    l.loan_name,
    l.loan_amount,
    l.loan_start_date,
    l.loan_end_date,
    l.paid_amount,
    p.payment_number,
    p.payment_date,
    p.payment_fact_date,
    p.paid_fact_amount,
    p.status
FROM core.clients c
JOIN core.loans l
    ON c.client_id = l.client_id
JOIN core.payments p
    ON l.client_id = p.client_id AND l.loan_name = p.loan_name
ON CONFLICT (client_id, loan_name, payment_number) DO NOTHING;


CREATE OR REPLACE VIEW vw_overdue_by_month_and_amount AS
WITH
  -------------------------------------------------------------------------------
  -- CTE 1: loans_cte
  --   для каждого платежа рассчитываем period (год-месяц) и bucket по сумме кредита
  -------------------------------------------------------------------------------
  loans_cte AS (
    SELECT
      client_id,
      loan_name,
      loan_amount,
      date_trunc('month', payment_date)::date AS period,
      CASE
        WHEN loan_amount < 50000  THEN 'До 50 000'
        WHEN loan_amount < 100000 THEN 'До 100 000'
        WHEN loan_amount < 500000 THEN 'До 500 000'
        ELSE 'От 500 000'
      END AS loan_amount_bucket
    FROM mart.data_mart
  ),
  -------------------------------------------------------------------------------
  -- CTE 2: overdue_cte
  --   выбираем из loans_cte только те «договор + месяц + bucket», где был хотя бы один платёж с status = TRUE
  -------------------------------------------------------------------------------
  overdue_cte AS (
    SELECT DISTINCT
      l.client_id,
      l.loan_name,
      l.loan_amount,
      l.period,
      l.loan_amount_bucket
    FROM loans_cte AS l
    JOIN mart.data_mart AS m
      ON m.client_id = l.client_id
     AND m.loan_name = l.loan_name
     AND date_trunc('month', m.payment_date)::date = l.period
     AND m.status = TRUE
  ),
  -------------------------------------------------------------------------------
  -- CTE 3: loans_monthly
  --   агрегируем ВСЕ активные договоры по (period, loan_amount_bucket)
  -------------------------------------------------------------------------------
  loans_monthly AS (
    SELECT
      period,
      loan_amount_bucket,
      COUNT(DISTINCT client_id || '|' || loan_name) AS total_loans,
      COUNT(DISTINCT client_id) AS total_clients,
      SUM(loan_amount) AS sum_issued_loans
    FROM loans_cte
    GROUP BY
      period,
      loan_amount_bucket
  ),
  -------------------------------------------------------------------------------
  -- CTE 4: overdue_monthly
  --   агрегируем ТОЛЬКО просроченные договоры по (period, loan_amount_bucket)
  --   total_overdue_loans = count distinct договоров,
  --   sum_overdue_payments = сумма paid_fact_amount для этих договоров в этом месяце
  -------------------------------------------------------------------------------
  overdue_monthly AS (
    SELECT
      o.period,
      o.loan_amount_bucket,
      COUNT(DISTINCT o.client_id || '|' || o.loan_name) AS total_overdue_loans,
      SUM(m.paid_fact_amount) FILTER (WHERE m.status = TRUE) AS sum_overdue_payments
    FROM overdue_cte AS o
    JOIN mart.data_mart AS m
      ON m.client_id = o.client_id
     AND m.loan_name = o.loan_name
     AND date_trunc('month', m.payment_date)::date = o.period
     AND m.status = TRUE
    GROUP BY
      o.period,
      o.loan_amount_bucket
  )

  -------------------------------------------------------------------------------
-- Финальный SELECT: объединяем loans_monthly и overdue_monthly
-------------------------------------------------------------------------------
SELECT
  COALESCE(l.period, o.period) AS period,
  EXTRACT(YEAR FROM COALESCE(l.period, o.period))::INT AS year,
  EXTRACT(MONTH FROM COALESCE(l.period, o.period))::INT AS month,
  COALESCE(l.loan_amount_bucket, o.loan_amount_bucket) AS loan_amount_bucket,
  -- Сколько ВСЕГО активных договоров (клиент+loan_name) было в этом месяце и bucket-е:
  COALESCE(l.total_loans, 0) AS total_loans,
  -- Сколько из них (тех же договоров) оказались просрочеными:
  COALESCE(o.total_overdue_loans, 0) AS total_overdue_loans,
  -- Доля просроченных договоров в процентах (0 если total_loans = 0):
  ROUND(100.0 * COALESCE(o.total_overdue_loans, 0) / NULLIF(COALESCE(l.total_loans, 0), 0), 2) AS pct_overdue_loans,
  -- Сумма всех выданных кредитов (loan_amount) в этом месяце и bucket-е:
  COALESCE(l.sum_issued_loans, 0) AS sum_issued_loans,
  -- Сумма всех просроченных фактических выплат в этом месяце и bucket-е:
  COALESCE(o.sum_overdue_payments, 0) AS sum_overdue_payments
FROM loans_monthly AS l
FULL OUTER JOIN overdue_monthly AS o
  ON l.period = o.period
 AND l.loan_amount_bucket = o.loan_amount_bucket
ORDER BY
  period,
  loan_amount_bucket;

