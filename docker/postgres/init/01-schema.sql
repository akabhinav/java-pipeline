-- ============================================================================
-- Enterprise Data Pipeline Platform - PostgreSQL Schema
-- ============================================================================

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- Loan Applications Table
-- ============================================================================
CREATE TABLE loan_applications (
    application_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    applicant_name VARCHAR(200) NOT NULL,
    applicant_age INTEGER NOT NULL CHECK (applicant_age >= 18 AND applicant_age <= 100),
    loan_amount DECIMAL(15,2) NOT NULL CHECK (loan_amount > 0),
    loan_term_months INTEGER NOT NULL CHECK (loan_term_months > 0),
    loan_purpose VARCHAR(50) NOT NULL,
    application_date DATE NOT NULL DEFAULT CURRENT_DATE,
    application_status VARCHAR(20) DEFAULT 'PENDING',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_loan_customer ON loan_applications(customer_id);
CREATE INDEX idx_loan_status ON loan_applications(application_status);
CREATE INDEX idx_loan_date ON loan_applications(application_date);

-- ============================================================================
-- Customers Table
-- ============================================================================
CREATE TABLE customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(20),
    date_of_birth DATE,
    gender VARCHAR(10),
    marital_status VARCHAR(20),
    address_line1 VARCHAR(500),
    address_line2 VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100) DEFAULT 'India',
    monthly_income DECIMAL(15,2),
    employment_status VARCHAR(50),
    employer_name VARCHAR(200),
    years_at_current_job INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_customer_email ON customers(email);
CREATE INDEX idx_customer_city ON customers(city);

-- ============================================================================
-- Credit Bureau Data Table
-- ============================================================================
CREATE TABLE credit_bureau_data (
    credit_record_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_id VARCHAR(50) NOT NULL REFERENCES customers(customer_id),
    credit_score INTEGER CHECK (credit_score >= 300 AND credit_score <= 850),
    total_credit_lines INTEGER DEFAULT 0,
    active_loans INTEGER DEFAULT 0,
    total_outstanding_debt DECIMAL(15,2) DEFAULT 0,
    monthly_debt_obligations DECIMAL(15,2) DEFAULT 0,
    credit_history_years INTEGER DEFAULT 0,
    default_history BOOLEAN DEFAULT FALSE,
    bankruptcy_history BOOLEAN DEFAULT FALSE,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_credit_customer ON credit_bureau_data(customer_id);
CREATE INDEX idx_credit_score ON credit_bureau_data(credit_score);

-- ============================================================================
-- Transactions Table
-- ============================================================================
CREATE TABLE transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL REFERENCES customers(customer_id),
    account_number VARCHAR(50),
    transaction_type VARCHAR(50) NOT NULL,
    amount DECIMAL(15,2) NOT NULL,
    currency VARCHAR(10) DEFAULT 'INR',
    merchant_name VARCHAR(200),
    merchant_category VARCHAR(100),
    transaction_location VARCHAR(200),
    transaction_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_international BOOLEAN DEFAULT FALSE,
    device_type VARCHAR(50),
    ip_address VARCHAR(50),
    status VARCHAR(20) DEFAULT 'COMPLETED',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_txn_customer ON transactions(customer_id);
CREATE INDEX idx_txn_timestamp ON transactions(transaction_timestamp);
CREATE INDEX idx_txn_type ON transactions(transaction_type);
CREATE INDEX idx_txn_amount ON transactions(amount);

-- ============================================================================
-- Loan Approval Results Table (Pipeline Output)
-- ============================================================================
CREATE TABLE loan_approval_results (
    result_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    application_id VARCHAR(50) NOT NULL REFERENCES loan_applications(application_id),
    customer_id VARCHAR(50) NOT NULL,
    loan_amount DECIMAL(15,2) NOT NULL,
    loan_term_months INTEGER NOT NULL,
    credit_score INTEGER,
    credit_score_grade VARCHAR(20),
    debt_to_income_ratio DECIMAL(5,4),
    loan_status VARCHAR(20),
    approval_reason TEXT,
    rejection_reason TEXT,
    monthly_emi DECIMAL(15,2),
    interest_rate DECIMAL(5,2),
    fraud_flag BOOLEAN DEFAULT FALSE,
    fraud_reason TEXT,
    fraud_score DECIMAL(5,2),
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_result_application ON loan_approval_results(application_id);
CREATE INDEX idx_result_status ON loan_approval_results(loan_status);
CREATE INDEX idx_result_fraud ON loan_approval_results(fraud_flag);

-- ============================================================================
-- Fraud Detection Results Table
-- ============================================================================
CREATE TABLE fraud_detection_results (
    fraud_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id VARCHAR(50) NOT NULL REFERENCES transactions(transaction_id),
    customer_id VARCHAR(50) NOT NULL,
    amount DECIMAL(15,2),
    fraud_flag BOOLEAN DEFAULT FALSE,
    fraud_score DECIMAL(5,2),
    fraud_reason TEXT,
    risk_level VARCHAR(20),
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fraud_transaction ON fraud_detection_results(transaction_id);
CREATE INDEX idx_fraud_flag ON fraud_detection_results(fraud_flag);
CREATE INDEX idx_fraud_risk ON fraud_detection_results(risk_level);

-- ============================================================================
-- Pipeline Execution Log Table
-- ============================================================================
CREATE TABLE pipeline_execution_log (
    execution_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_name VARCHAR(200) NOT NULL,
    execution_status VARCHAR(50) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    records_processed INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    error_message TEXT,
    execution_metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_pipeline_name ON pipeline_execution_log(pipeline_name);
CREATE INDEX idx_pipeline_status ON pipeline_execution_log(execution_status);
CREATE INDEX idx_pipeline_start ON pipeline_execution_log(start_time);

-- ============================================================================
-- Create views for reporting
-- ============================================================================

-- Loan Application Summary View
CREATE VIEW v_loan_application_summary AS
SELECT
    la.application_id,
    la.customer_id,
    c.name AS customer_name,
    c.email,
    c.city,
    la.loan_amount,
    la.loan_term_months,
    la.loan_purpose,
    la.application_status,
    cbd.credit_score,
    cbd.total_outstanding_debt,
    cbd.monthly_debt_obligations,
    c.monthly_income,
    CASE
        WHEN c.monthly_income > 0
        THEN ROUND((cbd.monthly_debt_obligations / c.monthly_income)::numeric, 4)
        ELSE NULL
    END AS debt_to_income_ratio,
    la.application_date
FROM loan_applications la
JOIN customers c ON la.customer_id = c.customer_id
LEFT JOIN credit_bureau_data cbd ON la.customer_id = cbd.customer_id;

-- Customer 360 View
CREATE VIEW v_customer_360 AS
SELECT
    c.customer_id,
    c.name,
    c.email,
    c.city,
    c.monthly_income,
    c.employment_status,
    cbd.credit_score,
    cbd.total_outstanding_debt,
    COUNT(DISTINCT t.transaction_id) AS total_transactions,
    COALESCE(SUM(t.amount), 0) AS total_transaction_amount,
    COALESCE(AVG(t.amount), 0) AS avg_transaction_amount,
    COALESCE(MAX(t.amount), 0) AS max_transaction_amount,
    COUNT(DISTINCT la.application_id) AS total_loan_applications,
    COUNT(DISTINCT CASE WHEN la.application_status = 'APPROVED' THEN la.application_id END) AS approved_loans
FROM customers c
LEFT JOIN credit_bureau_data cbd ON c.customer_id = cbd.customer_id
LEFT JOIN transactions t ON c.customer_id = t.customer_id
LEFT JOIN loan_applications la ON c.customer_id = la.customer_id
GROUP BY c.customer_id, c.name, c.email, c.city, c.monthly_income,
         c.employment_status, cbd.credit_score, cbd.total_outstanding_debt;

-- Fraud Statistics View
CREATE VIEW v_fraud_statistics AS
SELECT
    DATE(t.transaction_timestamp) AS transaction_date,
    COUNT(*) AS total_transactions,
    COUNT(CASE WHEN fdr.fraud_flag = TRUE THEN 1 END) AS fraud_transactions,
    ROUND(100.0 * COUNT(CASE WHEN fdr.fraud_flag = TRUE THEN 1 END) / COUNT(*), 2) AS fraud_percentage,
    SUM(t.amount) AS total_amount,
    SUM(CASE WHEN fdr.fraud_flag = TRUE THEN t.amount ELSE 0 END) AS fraud_amount
FROM transactions t
LEFT JOIN fraud_detection_results fdr ON t.transaction_id = fdr.transaction_id
GROUP BY DATE(t.transaction_timestamp)
ORDER BY transaction_date DESC;

COMMENT ON TABLE loan_applications IS 'Stores loan application information';
COMMENT ON TABLE customers IS 'Customer master data';
COMMENT ON TABLE credit_bureau_data IS 'Credit bureau information for customers';
COMMENT ON TABLE transactions IS 'All customer transactions';
COMMENT ON TABLE loan_approval_results IS 'Pipeline output for loan approvals';
COMMENT ON TABLE fraud_detection_results IS 'Fraud detection pipeline results';
COMMENT ON TABLE pipeline_execution_log IS 'Pipeline execution history';
