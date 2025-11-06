-- ============================================================================
-- Enterprise Data Pipeline Platform - Sample Data
-- ============================================================================

-- ============================================================================
-- Insert Sample Customers (15 records)
-- ============================================================================
INSERT INTO customers (customer_id, name, email, phone, date_of_birth, gender, marital_status,
                       address_line1, city, state, postal_code, country,
                       monthly_income, employment_status, employer_name, years_at_current_job)
VALUES
('C001', 'John Doe', 'john.doe@example.com', '+91-9876543210', '1988-05-15', 'Male', 'Married',
 '123 Marine Drive', 'Mumbai', 'Maharashtra', '400001', 'India',
 120000.00, 'FULL_TIME', 'Tech Corp', 5),

('C002', 'Jane Smith', 'jane.smith@example.com', '+91-9876543211', '1995-08-22', 'Female', 'Single',
 '456 Connaught Place', 'Delhi', 'Delhi', '110001', 'India',
 85000.00, 'FULL_TIME', 'Finance Ltd', 3),

('C003', 'Bob Johnson', 'bob.johnson@example.com', '+91-9876543212', '1978-03-10', 'Male', 'Married',
 '789 MG Road', 'Bangalore', 'Karnataka', '560001', 'India',
 200000.00, 'BUSINESS_OWNER', 'Self Employed', 10),

('C004', 'Alice Williams', 'alice.williams@example.com', '+91-9876543213', '1991-11-30', 'Female', 'Single',
 '321 FC Road', 'Pune', 'Maharashtra', '411001', 'India',
 95000.00, 'FULL_TIME', 'Software Inc', 4),

('C005', 'Charlie Brown', 'charlie.brown@example.com', '+91-9876543214', '1968-07-18', 'Male', 'Married',
 '654 Anna Salai', 'Chennai', 'Tamil Nadu', '600001', 'India',
 110000.00, 'SELF_EMPLOYED', 'Consultant', 8),

('C006', 'Diana Prince', 'diana.prince@example.com', '+91-9876543215', '1994-02-14', 'Female', 'Single',
 '987 Banjara Hills', 'Hyderabad', 'Telangana', '500034', 'India',
 130000.00, 'FULL_TIME', 'MNC Corp', 6),

('C007', 'Eve Anderson', 'eve.anderson@example.com', '+91-9876543216', '1997-09-05', 'Female', 'Single',
 '147 Park Street', 'Kolkata', 'West Bengal', '700016', 'India',
 60000.00, 'PART_TIME', 'Retail Store', 2),

('C008', 'Frank Miller', 'frank.miller@example.com', '+91-9876543217', '1972-12-20', 'Male', 'Married',
 '258 CG Road', 'Ahmedabad', 'Gujarat', '380009', 'India',
 250000.00, 'BUSINESS_OWNER', 'Manufacturing', 15),

('C009', 'Grace Lee', 'grace.lee@example.com', '+91-9876543218', '1997-06-08', 'Female', 'Single',
 '369 MI Road', 'Jaipur', 'Rajasthan', '302001', 'India',
 75000.00, 'FULL_TIME', 'Startup Inc', 2),

('C010', 'Henry Wilson', 'henry.wilson@example.com', '+91-9876543219', '1973-04-25', 'Male', 'Married',
 '741 Hazratganj', 'Lucknow', 'Uttar Pradesh', '226001', 'India',
 140000.00, 'FULL_TIME', 'Bank Ltd', 12),

('C011', 'Ivy Chen', 'ivy.chen@example.com', '+91-9876543220', '1990-10-12', 'Female', 'Married',
 '852 Ring Road', 'Surat', 'Gujarat', '395001', 'India',
 90000.00, 'FULL_TIME', 'Trading Co', 4),

('C012', 'Jack Taylor', 'jack.taylor@example.com', '+91-9876543221', '1967-01-30', 'Male', 'Married',
 '963 Rajwada', 'Indore', 'Madhya Pradesh', '452001', 'India',
 300000.00, 'BUSINESS_OWNER', 'Real Estate', 20),

('C013', 'Kate Davis', 'kate.davis@example.com', '+91-9876543222', '1992-08-17', 'Female', 'Single',
 '159 Sitabuldi', 'Nagpur', 'Maharashtra', '440012', 'India',
 100000.00, 'FULL_TIME', 'Engineering', 5),

('C014', 'Leo Martinez', 'leo.martinez@example.com', '+91-9876543223', '1984-03-22', 'Male', 'Married',
 '357 RS Puram', 'Coimbatore', 'Tamil Nadu', '641002', 'India',
 115000.00, 'FULL_TIME', 'Auto Corp', 7),

('C015', 'Mia Garcia', 'mia.garcia@example.com', '+91-9876543224', '1996-11-09', 'Female', 'Single',
 '486 MG Road', 'Kochi', 'Kerala', '682016', 'India',
 70000.00, 'FULL_TIME', 'Healthcare', 3);

-- ============================================================================
-- Insert Credit Bureau Data (15 records)
-- ============================================================================
INSERT INTO credit_bureau_data (customer_id, credit_score, total_credit_lines, active_loans,
                                 total_outstanding_debt, monthly_debt_obligations,
                                 credit_history_years, default_history, bankruptcy_history)
VALUES
('C001', 720, 5, 2, 300000.00, 25000.00, 10, FALSE, FALSE),
('C002', 680, 3, 1, 80000.00, 15000.00, 5, FALSE, FALSE),
('C003', 750, 8, 3, 500000.00, 40000.00, 15, FALSE, FALSE),
('C004', 700, 4, 2, 200000.00, 18000.00, 7, FALSE, FALSE),
('C005', 650, 6, 2, 150000.00, 20000.00, 12, FALSE, FALSE),
('C006', 740, 5, 1, 250000.00, 22000.00, 8, FALSE, FALSE),
('C007', 600, 2, 1, 40000.00, 8000.00, 3, TRUE, FALSE),
('C008', 780, 10, 4, 800000.00, 60000.00, 20, FALSE, FALSE),
('C009', 660, 3, 2, 120000.00, 12000.00, 4, FALSE, FALSE),
('C010', 730, 6, 2, 350000.00, 30000.00, 14, FALSE, FALSE),
('C011', 690, 4, 1, 90000.00, 16000.00, 6, FALSE, FALSE),
('C012', 800, 12, 5, 1200000.00, 80000.00, 25, FALSE, FALSE),
('C013', 710, 5, 2, 180000.00, 20000.00, 9, FALSE, FALSE),
('C014', 725, 6, 3, 280000.00, 24000.00, 11, FALSE, FALSE),
('C015', 670, 3, 1, 60000.00, 12000.00, 4, FALSE, FALSE);

-- ============================================================================
-- Insert Loan Applications (15 records)
-- ============================================================================
INSERT INTO loan_applications (application_id, customer_id, applicant_name, applicant_age,
                                loan_amount, loan_term_months, loan_purpose, application_date, application_status)
VALUES
('LA001', 'C001', 'John Doe', 35, 500000.00, 240, 'HOME_LOAN', '2024-01-15', 'PENDING'),
('LA002', 'C002', 'Jane Smith', 28, 150000.00, 60, 'PERSONAL_LOAN', '2024-01-16', 'PENDING'),
('LA003', 'C003', 'Bob Johnson', 45, 2000000.00, 180, 'BUSINESS_LOAN', '2024-01-17', 'PENDING'),
('LA004', 'C004', 'Alice Williams', 32, 800000.00, 84, 'CAR_LOAN', '2024-01-18', 'PENDING'),
('LA005', 'C005', 'Charlie Brown', 55, 300000.00, 36, 'PERSONAL_LOAN', '2024-01-19', 'PENDING'),
('LA006', 'C006', 'Diana Prince', 29, 1200000.00, 240, 'HOME_LOAN', '2024-01-20', 'PENDING'),
('LA007', 'C007', 'Eve Anderson', 26, 50000.00, 12, 'PERSONAL_LOAN', '2024-01-21', 'PENDING'),
('LA008', 'C008', 'Frank Miller', 51, 3000000.00, 120, 'BUSINESS_LOAN', '2024-01-22', 'PENDING'),
('LA009', 'C009', 'Grace Lee', 26, 600000.00, 60, 'CAR_LOAN', '2024-01-23', 'PENDING'),
('LA010', 'C010', 'Henry Wilson', 50, 1500000.00, 180, 'HOME_LOAN', '2024-01-24', 'PENDING'),
('LA011', 'C011', 'Ivy Chen', 33, 200000.00, 24, 'PERSONAL_LOAN', '2024-01-25', 'PENDING'),
('LA012', 'C012', 'Jack Taylor', 56, 5000000.00, 240, 'BUSINESS_LOAN', '2024-01-26', 'PENDING'),
('LA013', 'C013', 'Kate Davis', 31, 450000.00, 48, 'CAR_LOAN', '2024-01-27', 'PENDING'),
('LA014', 'C014', 'Leo Martinez', 39, 900000.00, 120, 'HOME_LOAN', '2024-01-28', 'PENDING'),
('LA015', 'C015', 'Mia Garcia', 27, 75000.00, 18, 'PERSONAL_LOAN', '2024-01-29', 'PENDING');

-- ============================================================================
-- Insert Transactions (30 records - 2 per customer)
-- ============================================================================
INSERT INTO transactions (transaction_id, customer_id, account_number, transaction_type, amount,
                          merchant_name, merchant_category, transaction_location,
                          transaction_timestamp, is_international, device_type)
VALUES
-- Customer C001
('TXN001', 'C001', 'ACC001001', 'DEBIT', 5000.00, 'SuperMart', 'GROCERY', 'Mumbai', '2024-01-15 10:30:00', FALSE, 'MOBILE'),
('TXN002', 'C001', 'ACC001001', 'DEBIT', 250000.00, 'Jewelry Store', 'JEWELRY', 'Dubai', '2024-01-15 15:00:00', TRUE, 'POS'),

-- Customer C002
('TXN003', 'C002', 'ACC002001', 'DEBIT', 150000.00, 'ElectroWorld', 'ELECTRONICS', 'Delhi', '2024-01-16 11:00:00', FALSE, 'WEB'),
('TXN004', 'C002', 'ACC002001', 'DEBIT', 300000.00, 'Luxury Mall', 'LUXURY', 'Singapore', '2024-01-16 16:00:00', TRUE, 'WEB'),

-- Customer C003
('TXN005', 'C003', 'ACC003001', 'TRANSFER', 500000.00, 'Business Partner', 'BUSINESS', 'Bangalore', '2024-01-17 12:00:00', FALSE, 'BRANCH'),
('TXN006', 'C003', 'ACC003001', 'DEBIT', 400000.00, 'Designer Store', 'LUXURY', 'London', '2024-01-17 22:00:00', TRUE, 'POS'),

-- Customer C004
('TXN007', 'C004', 'ACC004001', 'DEBIT', 15000.00, 'Shell Petrol', 'FUEL', 'Pune', '2024-01-18 14:00:00', FALSE, 'MOBILE'),
('TXN008', 'C004', 'ACC004001', 'DEBIT', 8000.00, 'Pizza Hut', 'RESTAURANT', 'Pune', '2024-01-18 20:00:00', FALSE, 'MOBILE'),

-- Customer C005
('TXN009', 'C005', 'ACC005001', 'DEBIT', 8000.00, 'Taj Restaurant', 'RESTAURANT', 'Chennai', '2024-01-19 15:00:00', FALSE, 'MOBILE'),
('TXN010', 'C005', 'ACC005001', 'DEBIT', 12000.00, 'Book Store', 'SHOPPING', 'Chennai', '2024-01-19 18:00:00', FALSE, 'WEB'),

-- Customer C006
('TXN011', 'C006', 'ACC006001', 'DEBIT', 12000.00, 'Big Bazaar', 'GROCERY', 'Hyderabad', '2024-01-20 17:00:00', FALSE, 'POS'),
('TXN012', 'C006', 'ACC006001', 'DEBIT', 25000.00, 'Fashion Store', 'SHOPPING', 'Hyderabad', '2024-01-20 19:00:00', FALSE, 'WEB'),

-- Customer C007
('TXN013', 'C007', 'ACC007001', 'DEBIT', 3000.00, 'Cinema Hall', 'ENTERTAINMENT', 'Kolkata', '2024-01-21 18:00:00', FALSE, 'MOBILE'),
('TXN014', 'C007', 'ACC007001', 'DEBIT', 5000.00, 'Cafe Coffee Day', 'RESTAURANT', 'Kolkata', '2024-01-21 21:00:00', FALSE, 'MOBILE'),

-- Customer C008
('TXN015', 'C008', 'ACC008001', 'TRANSFER', 750000.00, 'Supplier Payment', 'BUSINESS', 'Ahmedabad', '2024-01-22 19:00:00', FALSE, 'BRANCH'),
('TXN016', 'C008', 'ACC008001', 'DEBIT', 100000.00, 'Equipment Store', 'BUSINESS', 'Ahmedabad', '2024-01-22 14:00:00', FALSE, 'BRANCH'),

-- Customer C009
('TXN017', 'C009', 'ACC009001', 'DEBIT', 25000.00, 'Zara', 'SHOPPING', 'Jaipur', '2024-01-23 20:00:00', FALSE, 'WEB'),
('TXN018', 'C009', 'ACC009001', 'DEBIT', 8000.00, 'McDonald', 'RESTAURANT', 'Jaipur', '2024-01-23 13:00:00', FALSE, 'MOBILE'),

-- Customer C010
('TXN019', 'C010', 'ACC010001', 'DEBIT', 6000.00, 'Reliance Fresh', 'GROCERY', 'Lucknow', '2024-01-24 21:00:00', FALSE, 'MOBILE'),
('TXN020', 'C010', 'ACC010001', 'DEBIT', 45000.00, 'Electronics Mart', 'ELECTRONICS', 'Lucknow', '2024-01-24 16:00:00', FALSE, 'WEB'),

-- Customer C011
('TXN021', 'C011', 'ACC011001', 'DEBIT', 18000.00, 'Samsung Store', 'ELECTRONICS', 'Surat', '2024-01-25 23:00:00', FALSE, 'WEB'),
('TXN022', 'C011', 'ACC011001', 'DEBIT', 7000.00, 'D-Mart', 'GROCERY', 'Surat', '2024-01-25 10:00:00', FALSE, 'POS'),

-- Customer C012
('TXN023', 'C012', 'ACC012001', 'TRANSFER', 1000000.00, 'Property Deal', 'REAL_ESTATE', 'Indore', '2024-01-26 00:00:00', FALSE, 'BRANCH'),
('TXN024', 'C012', 'ACC012001', 'DEBIT', 200000.00, 'Car Showroom', 'AUTOMOBILE', 'Indore', '2024-01-26 15:00:00', FALSE, 'BRANCH'),

-- Customer C013
('TXN025', 'C013', 'ACC013001', 'DEBIT', 18000.00, 'Pantaloons', 'SHOPPING', 'Nagpur', '2024-01-27 18:00:00', FALSE, 'WEB'),
('TXN026', 'C013', 'ACC013001', 'DEBIT', 9000.00, 'HP Petrol', 'FUEL', 'Nagpur', '2024-01-27 08:00:00', FALSE, 'MOBILE'),

-- Customer C014
('TXN027', 'C014', 'ACC014001', 'DEBIT', 6000.00, 'More Supermarket', 'GROCERY', 'Coimbatore', '2024-01-28 09:00:00', FALSE, 'MOBILE'),
('TXN028', 'C014', 'ACC014001', 'DEBIT', 50000.00, 'Croma', 'ELECTRONICS', 'Coimbatore', '2024-01-28 17:00:00', FALSE, 'WEB'),

-- Customer C015
('TXN029', 'C015', 'ACC015001', 'DEBIT', 12000.00, 'Westside', 'SHOPPING', 'Kochi', '2024-01-29 19:00:00', FALSE, 'WEB'),
('TXN030', 'C015', 'ACC015001', 'DEBIT', 4000.00, 'Starbucks', 'RESTAURANT', 'Kochi', '2024-01-29 11:00:00', FALSE, 'MOBILE');

-- ============================================================================
-- Summary Statistics
-- ============================================================================
SELECT 'Data Loading Complete!' AS status;
SELECT 'Customers: ' || COUNT(*) FROM customers;
SELECT 'Loan Applications: ' || COUNT(*) FROM loan_applications;
SELECT 'Credit Records: ' || COUNT(*) FROM credit_bureau_data;
SELECT 'Transactions: ' || COUNT(*) FROM transactions;
