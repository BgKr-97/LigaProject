DROP TABLE IF EXISTS temp_loans;
CREATE TEMPORARY TABLE temp_loans
(
    client_id       INTEGER   NOT NULL,
    loan_name       TEXT      NOT NULL,
    loan_amount     INTEGER   NOT NULL,
    loan_start_date TIMESTAMP NOT NULL,
    loan_end_date   TIMESTAMP NOT NULL,
    payment_numbers SMALLINT  NOT NULL,
    paid_amount     INTEGER   NOT NULL,
    file_name       TEXT      NOT NULL,
    load_ts         TIMESTAMP NOT NULL
);
