--------------------------------------------------------------------------------
-- Initialize Data Lake Catalog in Trino
--
-- This script registers Delta Lake tables from MinIO in the Hive Metastore,
-- making them queryable via Trino SQL.
--
-- Usage: Run this after Data Lake pipeline creates the Delta tables
--   docker exec -it pipeline-trino trino --catalog delta --schema default
--   Then paste this script
--------------------------------------------------------------------------------

-- Create database/schema for Data Lake
CREATE SCHEMA IF NOT EXISTS delta.datalake
WITH (location = 's3a://data-lake/');

-- ============================================================================
-- BRONZE LAYER TABLES
-- ============================================================================

-- Loan Applications (Bronze - Raw)
CREATE TABLE IF NOT EXISTS delta.datalake.loan_applications_bronze (
    application_id VARCHAR,
    customer_id VARCHAR,
    applicant_name VARCHAR,
    loan_amount DOUBLE,
    loan_purpose VARCHAR,
    loan_term_months INTEGER,
    applicant_age INTEGER,
    monthly_income DOUBLE,
    employment_status VARCHAR,
    application_date VARCHAR,
    ingestion_timestamp TIMESTAMP,
    source_system VARCHAR
)
WITH (
    location = 's3a://data-lake/bronze/loan_applications',
    format = 'DELTA'
);

-- Customers (Bronze - Raw)
CREATE TABLE IF NOT EXISTS delta.datalake.customers_bronze (
    customer_id VARCHAR,
    name VARCHAR,
    email VARCHAR,
    monthly_income DOUBLE,
    employment_status VARCHAR,
    city VARCHAR,
    state VARCHAR,
    ingestion_timestamp TIMESTAMP,
    source_system VARCHAR
)
WITH (
    location = 's3a://data-lake/bronze/customers',
    format = 'DELTA'
);

-- ============================================================================
-- SILVER LAYER TABLES (Cleaned & Validated)
-- ============================================================================

-- Loan Applications (Silver - Cleaned)
CREATE TABLE IF NOT EXISTS delta.datalake.loan_applications_silver (
    application_id VARCHAR,
    customer_id VARCHAR,
    applicant_name VARCHAR,
    loan_amount DOUBLE,
    loan_purpose VARCHAR,
    loan_term_months INTEGER,
    applicant_age INTEGER,
    monthly_income DOUBLE,
    employment_status VARCHAR,
    application_date VARCHAR,
    data_quality_score DOUBLE,
    cleaned_timestamp TIMESTAMP
)
WITH (
    location = 's3a://data-lake/silver/loan_applications_clean',
    format = 'DELTA'
);

-- Customers (Silver - Cleaned)
CREATE TABLE IF NOT EXISTS delta.datalake.customers_silver (
    customer_id VARCHAR,
    name VARCHAR,
    email VARCHAR,
    monthly_income DOUBLE,
    employment_status VARCHAR,
    city VARCHAR,
    state VARCHAR,
    data_quality_score DOUBLE,
    cleaned_timestamp TIMESTAMP
)
WITH (
    location = 's3a://data-lake/silver/customers_clean',
    format = 'DELTA'
);

-- ============================================================================
-- GOLD LAYER TABLES (Business Aggregates)
-- ============================================================================

-- Loan Summary by Purpose
CREATE TABLE IF NOT EXISTS delta.datalake.loan_summary_gold (
    loan_purpose VARCHAR,
    total_applications BIGINT,
    total_amount DOUBLE,
    avg_amount DOUBLE,
    avg_applicant_age DOUBLE,
    updated_at TIMESTAMP
)
WITH (
    location = 's3a://data-lake/gold/loan_summary_by_purpose',
    format = 'DELTA'
);

-- Customer Metrics
CREATE TABLE IF NOT EXISTS delta.datalake.customer_metrics_gold (
    employment_status VARCHAR,
    customer_count BIGINT,
    avg_monthly_income DOUBLE,
    total_monthly_income DOUBLE,
    updated_at TIMESTAMP
)
WITH (
    location = 's3a://data-lake/gold/customer_metrics',
    format = 'DELTA'
);

-- ============================================================================
-- Verify Tables Created
-- ============================================================================

SHOW TABLES FROM delta.datalake;
