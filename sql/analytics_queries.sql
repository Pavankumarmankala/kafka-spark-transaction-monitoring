-- Real-Time Kafka + Spark Transaction Monitoring
-- Dialect: Amazon Athena / Redshift (standard SQL).
-- Tables: transactions_clean (deduplicated, scored stream output),
--         suspicious_alerts (subset where is_suspicious = 1)

-- 1. Hourly transaction volume and suspicious rate
SELECT date_trunc('hour', event_time) AS event_hour,
       COUNT(*) AS transactions,
       SUM(is_suspicious) AS suspicious,
       ROUND(100.0 * SUM(is_suspicious) / COUNT(*), 2) AS suspicious_rate_pct,
       SUM(amount) AS revenue
FROM transactions_clean GROUP BY 1 ORDER BY 1;

-- 2. Suspicious alerts by rule
SELECT suspicious_reasons, COUNT(*) AS alerts, SUM(amount) AS amount_flagged
FROM suspicious_alerts
GROUP BY 1 ORDER BY alerts DESC;

-- 3. Suspicious rate by country (foreign-payment exposure)
SELECT country, COUNT(*) AS transactions, SUM(is_suspicious) AS suspicious,
       ROUND(100.0 * SUM(is_suspicious) / COUNT(*), 2) AS suspicious_rate_pct
FROM transactions_clean GROUP BY 1 ORDER BY suspicious_rate_pct DESC;

-- 4. Highest-risk customers (top 50 by suspicious transactions)
SELECT customer_id, COUNT(*) AS transactions,
       SUM(is_suspicious) AS suspicious_txns,
       SUM(CASE WHEN is_suspicious = 1 THEN amount ELSE 0 END) AS amount_at_risk,
       MAX(amount) AS largest_txn
FROM transactions_clean GROUP BY 1
HAVING SUM(is_suspicious) > 0
ORDER BY suspicious_txns DESC LIMIT 50;

-- 5. Velocity suspects: customers with bursts flagged for velocity
SELECT customer_id, CAST(event_time AS DATE) AS event_date,
       COUNT(*) AS flagged_txns, SUM(amount) AS burst_amount
FROM suspicious_alerts
WHERE suspicious_reasons LIKE '%velocity%'
GROUP BY 1, 2 ORDER BY flagged_txns DESC LIMIT 50;

-- 6. Daily revenue monitored vs revenue at risk
SELECT CAST(event_time AS DATE) AS event_date,
       COUNT(*) AS transactions,
       SUM(amount) AS revenue_monitored,
       SUM(CASE WHEN is_suspicious = 1 THEN amount ELSE 0 END) AS revenue_at_risk,
       ROUND(100.0 * SUM(CASE WHEN is_suspicious = 1 THEN amount ELSE 0 END)
             / SUM(amount), 2) AS pct_revenue_at_risk
FROM transactions_clean GROUP BY 1 ORDER BY 1;

-- 7. Payment-method risk profile
SELECT payment_method, COUNT(*) AS transactions, SUM(is_suspicious) AS suspicious,
       ROUND(100.0 * SUM(is_suspicious) / COUNT(*), 2) AS suspicious_rate_pct
FROM transactions_clean GROUP BY 1 ORDER BY suspicious_rate_pct DESC;

-- 8. Largest amount anomalies for analyst review
SELECT transaction_id, customer_id, event_time, amount, country, suspicious_reasons
FROM suspicious_alerts
WHERE suspicious_reasons LIKE '%amount_anomaly%'
ORDER BY amount DESC LIMIT 50;
