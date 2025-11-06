--------------------------------------------------------------------------------
-- Trino SQL Query Examples for Data Lake
--
-- These queries demonstrate querying your Data Lake using Trino SQL.
--
-- Prerequisites:
--   1. Start environment: ./start-local-environment.sh
--   2. Run Data Lake pipeline: ./run-data-lake.sh
--   3. Initialize catalog: See CATALOG_GUIDE.md
--
-- Execute queries:
--   docker exec -it pipeline-trino trino --catalog delta --schema datalake
--   Then copy/paste queries below
--------------------------------------------------------------------------------

-- ============================================================================
-- BASIC QUERIES
-- ============================================================================

-- Show all tables
SHOW TABLES;

-- Show columns in a table
DESCRIBE loan_applications_silver;

-- Count records in each layer
SELECT 'Bronze' as layer, COUNT(*) as record_count
FROM loan_applications_bronze
UNION ALL
SELECT 'Silver' as layer, COUNT(*) as record_count
FROM loan_applications_silver
UNION ALL
SELECT 'Gold Summary' as layer, COUNT(*) as record_count
FROM loan_summary_gold;

-- ============================================================================
-- SILVER LAYER QUERIES (Cleaned Data)
-- ============================================================================

-- View all loan applications
SELECT * FROM loan_applications_silver LIMIT 10;

-- High-value loans (> $500,000)
SELECT
    application_id,
    applicant_name,
    loan_amount,
    loan_purpose,
    applicant_age
FROM loan_applications_silver
WHERE loan_amount > 500000
ORDER BY loan_amount DESC;

-- Loans by purpose
SELECT
    loan_purpose,
    COUNT(*) as application_count,
    SUM(loan_amount) as total_amount,
    AVG(loan_amount) as avg_amount,
    MIN(loan_amount) as min_amount,
    MAX(loan_amount) as max_amount
FROM loan_applications_silver
GROUP BY loan_purpose
ORDER BY total_amount DESC;

-- Loans by age group
SELECT
    CASE
        WHEN applicant_age < 30 THEN '< 30'
        WHEN applicant_age BETWEEN 30 AND 40 THEN '30-40'
        WHEN applicant_age BETWEEN 41 AND 50 THEN '41-50'
        ELSE '> 50'
    END as age_group,
    COUNT(*) as loan_count,
    AVG(loan_amount) as avg_loan_amount
FROM loan_applications_silver
GROUP BY 1
ORDER BY 1;

-- ============================================================================
-- JOIN QUERIES
-- ============================================================================

-- Enrich loans with customer data
SELECT
    l.application_id,
    l.applicant_name,
    l.loan_amount,
    l.loan_purpose,
    c.monthly_income,
    c.employment_status,
    ROUND(l.loan_amount / c.monthly_income, 2) as loan_to_income_ratio
FROM loan_applications_silver l
JOIN customers_silver c ON l.customer_id = c.customer_id
ORDER BY loan_to_income_ratio DESC;

-- Customers with multiple loan applications
SELECT
    c.customer_id,
    c.name,
    c.employment_status,
    COUNT(l.application_id) as loan_count,
    SUM(l.loan_amount) as total_loan_amount,
    AVG(l.loan_amount) as avg_loan_amount
FROM customers_silver c
JOIN loan_applications_silver l ON c.customer_id = l.customer_id
GROUP BY c.customer_id, c.name, c.employment_status
HAVING COUNT(l.application_id) > 1
ORDER BY total_loan_amount DESC;

-- ============================================================================
-- GOLD LAYER QUERIES (Business Metrics)
-- ============================================================================

-- View loan summary (already aggregated)
SELECT * FROM loan_summary_gold
ORDER BY total_amount DESC;

-- View customer metrics
SELECT * FROM customer_metrics_gold
ORDER BY avg_monthly_income DESC;

-- ============================================================================
-- ADVANCED ANALYTICS
-- ============================================================================

-- Loan approval risk analysis (by income and employment)
SELECT
    employment_status,
    CASE
        WHEN loan_amount / monthly_income < 3 THEN 'Low Risk'
        WHEN loan_amount / monthly_income BETWEEN 3 AND 5 THEN 'Medium Risk'
        ELSE 'High Risk'
    END as risk_category,
    COUNT(*) as application_count,
    AVG(loan_amount) as avg_loan_amount,
    AVG(monthly_income) as avg_monthly_income
FROM loan_applications_silver
GROUP BY employment_status, 2
ORDER BY employment_status, application_count DESC;

-- Loan distribution by purpose and age
SELECT
    loan_purpose,
    CASE
        WHEN applicant_age < 30 THEN '< 30'
        WHEN applicant_age BETWEEN 30 AND 40 THEN '30-40'
        WHEN applicant_age BETWEEN 41 AND 50 THEN '41-50'
        ELSE '> 50'
    END as age_group,
    COUNT(*) as loan_count,
    SUM(loan_amount) as total_amount,
    AVG(loan_amount) as avg_amount
FROM loan_applications_silver
GROUP BY loan_purpose, 2
ORDER BY loan_purpose, age_group;

-- Top 10 highest loan-to-income ratios
SELECT
    l.application_id,
    l.applicant_name,
    l.loan_amount,
    c.monthly_income,
    ROUND(l.loan_amount / c.monthly_income, 2) as loan_to_income_ratio,
    l.employment_status,
    l.loan_purpose
FROM loan_applications_silver l
JOIN customers_silver c ON l.customer_id = c.customer_id
ORDER BY loan_to_income_ratio DESC
LIMIT 10;

-- ============================================================================
-- DATA QUALITY CHECKS
-- ============================================================================

-- Check data quality scores
SELECT
    ROUND(data_quality_score, 2) as quality_score,
    COUNT(*) as record_count
FROM loan_applications_silver
GROUP BY 1
ORDER BY 1 DESC;

-- Compare Bronze vs Silver record counts (data loss during cleaning)
SELECT
    'Bronze (Raw)' as layer,
    COUNT(*) as record_count
FROM loan_applications_bronze
UNION ALL
SELECT
    'Silver (Clean)' as layer,
    COUNT(*) as record_count
FROM loan_applications_silver;

-- ============================================================================
-- TIME-BASED QUERIES
-- ============================================================================

-- Latest ingestion timestamp
SELECT
    MAX(ingestion_timestamp) as latest_ingestion,
    MIN(ingestion_timestamp) as earliest_ingestion,
    COUNT(*) as total_records
FROM loan_applications_bronze;

-- Records ingested today
SELECT
    application_id,
    applicant_name,
    loan_amount,
    ingestion_timestamp
FROM loan_applications_bronze
WHERE DATE(ingestion_timestamp) = CURRENT_DATE
ORDER BY ingestion_timestamp DESC;

-- ============================================================================
-- AGGREGATION & WINDOW FUNCTIONS
-- ============================================================================

-- Rank loans by amount within each purpose
SELECT
    application_id,
    applicant_name,
    loan_purpose,
    loan_amount,
    RANK() OVER (PARTITION BY loan_purpose ORDER BY loan_amount DESC) as rank_within_purpose
FROM loan_applications_silver
ORDER BY loan_purpose, rank_within_purpose;

-- Running total of loan amounts by purpose
SELECT
    application_id,
    loan_purpose,
    loan_amount,
    SUM(loan_amount) OVER (
        PARTITION BY loan_purpose
        ORDER BY application_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as running_total
FROM loan_applications_silver
ORDER BY loan_purpose, application_id;

-- ============================================================================
-- EXPORT RESULTS
-- ============================================================================

-- Export high-value loans to CSV (via Trino)
-- Note: This requires additional configuration for file output
SELECT
    application_id,
    applicant_name,
    loan_amount,
    loan_purpose,
    applicant_age,
    employment_status
FROM loan_applications_silver
WHERE loan_amount > 500000
ORDER BY loan_amount DESC;

-- ============================================================================
-- CATALOG EXPLORATION
-- ============================================================================

-- Show all catalogs
SHOW CATALOGS;

-- Show schemas in delta catalog
SHOW SCHEMAS FROM delta;

-- Show all tables in datalake schema
SHOW TABLES FROM delta.datalake;

-- Get table statistics
SHOW STATS FOR loan_applications_silver;

-- ============================================================================
-- PERFORMANCE TIPS
-- ============================================================================

-- Use EXPLAIN to see query plan
EXPLAIN SELECT * FROM loan_applications_silver WHERE loan_amount > 500000;

-- Use EXPLAIN ANALYZE to see actual execution stats
EXPLAIN ANALYZE
SELECT loan_purpose, COUNT(*)
FROM loan_applications_silver
GROUP BY loan_purpose;

--------------------------------------------------------------------------------
-- Quick Reference
--------------------------------------------------------------------------------
-- Connect to Trino:   docker exec -it pipeline-trino trino
-- List catalogs:      SHOW CATALOGS;
-- Use catalog:        USE delta.datalake;
-- List tables:        SHOW TABLES;
-- Query table:        SELECT * FROM table_name LIMIT 10;
-- Exit:               quit;
--------------------------------------------------------------------------------
