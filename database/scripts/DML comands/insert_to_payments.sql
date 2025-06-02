-- Переносим платежи из staging.payments в core.payments.
-- При конфликте по (client_id, loan_name, payment_number) — пропускаем.

INSERT INTO core.payments
(
    client_id,
    loan_name,
    payment_number,
    payment_date,
    payment_fact_date,
    paid_fact_amount,
    status
)
SELECT
    sp.client_id,
    sp.loan_name,
    sp.payment_number,
    sp.payment_date,
    sp.payment_fact_date,
    sp.paid_fact_amount,
    CASE
        WHEN sp.payment_fact_date > sp.payment_date OR sp.paid_fact_amount < cl.paid_amount THEN TRUE
        ELSE FALSE
    END AS status
FROM staging.payments AS sp
JOIN core.loans AS cl
    ON sp.client_id = cl.client_id AND sp.loan_name = cl.loan_name
ON CONFLICT (client_id, loan_name, payment_number) DO NOTHING;
