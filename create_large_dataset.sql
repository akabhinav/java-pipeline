-- Generate 10,000 loan applications
INSERT INTO loan_applications (loan_id, customer_id, loan_amount, loan_term_months, interest_rate, application_date, application_status)
SELECT 
    'L' || LPAD(generate_series::TEXT, 6, '0'),
    'C' || LPAD(((generate_series % 100) + 1)::TEXT, 4, '0'),
    50000 + (RANDOM() * 450000)::INT,
    12 * (1 + (RANDOM() * 4)::INT),
    5.0 + (RANDOM() * 10),
    CURRENT_DATE - (RANDOM() * 365)::INT,
    (ARRAY['PENDING', 'APPROVED', 'REJECTED'])[1 + (RANDOM() * 2)::INT]
FROM generate_series(16, 10000);
