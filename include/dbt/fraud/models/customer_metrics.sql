with transaction_summary AS(
    SELECT
        ct.user_id,
        COUNT(ct.transaction_id) as total_transaction,
        SUM(CASE WHEN lt.is_fraudulent THEN 1 ELSE 0 END) AS fraudulent_transactions,
        SUM(CASE WHEN NOT lt.is_fraudulent THEN 1 ELSE 0 END) AS non_fraudulent_transactions
    FROM staging.customer_transactions ct
    JOIN staging.labeled_transactions lt 
    ON ct.transaction_id = lt.transaction_id
    GROUP BY ct.user_id
)
SELECT
    user_id,
    total_transaction,
    fraudulent_transactions
    non_fraudulent_transactions,
    (fraudulent_transactions::FLOAT / total_transaction) * 100 AS risk_score
FROM transaction_summary;
