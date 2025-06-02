INSERT INTO staging.payments
(
    client_id,
    loan_name,
    payment_number,
    payment_date,
    payment_fact_date,
    paid_fact_amount,
    file_name,
    load_ts
)
SELECT
    t.client_id,
    t.loan_name,
    t.payment_number,
    t.payment_date,
    t.payment_fact_date,
    t.paid_fact_amount,
    t.file_name,
    t.load_ts
FROM temp_payments AS t
ON CONFLICT (client_id, loan_name, payment_number) DO NOTHING;
