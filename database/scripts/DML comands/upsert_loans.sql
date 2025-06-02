INSERT INTO staging.loans
(
    client_id,
    loan_name,
    loan_amount,
    loan_start_date,
    loan_end_date,
    payment_numbers,
    paid_amount,
    file_name,
    load_ts
)
SELECT
    t.client_id,
    t.loan_name,
    t.loan_amount,
    t.loan_start_date,
    t.loan_end_date,
    t.payment_numbers,
    t.paid_amount,
    t.file_name,
    t.load_ts
FROM temp_loans AS t
ON CONFLICT (client_id, loan_name) DO NOTHING;
