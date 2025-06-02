-- Переносим кредиты из staging.loans в core.loans.
-- При конфликте по (client_id, loan_name) НЕ вставляем повторно.

INSERT INTO core.loans
(
    client_id,
    loan_name,
    loan_amount,
    loan_start_date,
    loan_end_date,
    payment_numbers,
    paid_amount
)
SELECT
    s.client_id,
    s.loan_name,
    s.loan_amount,
    s.loan_start_date,
    s.loan_end_date,
    s.payment_numbers,
    s.paid_amount
FROM staging.loans AS s
ON CONFLICT (client_id, loan_name) DO NOTHING;
