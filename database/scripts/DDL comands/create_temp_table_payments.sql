DROP TABLE IF EXISTS temp_payments;
CREATE TEMPORARY TABLE temp_payments
(
    client_id         INTEGER   NOT NULL,
    loan_name         TEXT      NOT NULL,
    payment_number    SMALLINT  NOT NULL,
    payment_date      TIMESTAMP NOT NULL,
    payment_fact_date TIMESTAMP NOT NULL,
    paid_fact_amount  INTEGER   NOT NULL,
    file_name         TEXT      NOT NULL,
    load_ts           TIMESTAMP NOT NULL
);
