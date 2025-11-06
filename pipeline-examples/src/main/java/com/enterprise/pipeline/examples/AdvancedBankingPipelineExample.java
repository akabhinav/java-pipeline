package com.enterprise.pipeline.examples;

import com.enterprise.pipeline.api.PipelineException;
import com.enterprise.pipeline.api.model.PipelineConfig;
import com.enterprise.pipeline.core.builder.PipelineBuilder;
import com.enterprise.pipeline.core.engine.PipelineEngine;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Advanced banking pipeline demonstrating Phase 2 features.
 *
 * This example showcases:
 * 1. Window functions (row_number, rank, running totals)
 * 2. Specialized banking transformations
 *    - Account validation
 * - Credit card masking
 *    - Interest calculation
 *    - Fraud detection
 *    - Credit score calculation
 * 3. Complex analytics with multiple pipelines
 * 4. PCI compliance and data security
 *
 * Business Scenario:
 * - Process customer accounts and loan applications
 * - Calculate credit scores based on banking behavior
 * - Detect fraudulent transactions
 * - Calculate interest for loans
 * - Mask sensitive data for reporting
 * - Generate customer rankings and analytics
 *
 * @author Enterprise Data Pipeline Team
 */
public class AdvancedBankingPipelineExample {

    private static final Logger logger = LoggerFactory.getLogger(AdvancedBankingPipelineExample.class);

    public static void main(String[] args) {
        logger.info("Starting Advanced Banking Pipeline Example (Phase 2)");

        SparkSession spark = SparkSession.builder()
                .appName("Advanced Banking Pipeline")
                .master("local[*]")
                .config("spark.sql.adaptive.enabled", "true")
                .getOrCreate();

        try {
            // Create sample data
            createAdvancedBankingData(spark);

            // Execute pipelines
            processCustomerAccounts(spark);
            processTransactionsWithFraud Detection(spark);
            processLoanApplications(spark);
            generateCustomerRankings(spark);

            // Show results
            displayResults(spark);

        } catch (Exception e) {
            logger.error("Advanced banking pipeline failed", e);
            System.exit(1);
        } finally {
            spark.stop();
        }
    }

    /**
     * Pipeline 1: Process customer accounts and calculate credit scores.
     */
    private static void processCustomerAccounts(SparkSession spark) throws PipelineException {
        logger.info("=== Pipeline 1: Customer Account Processing ===");

        PipelineConfig config = PipelineBuilder.create("customer-account-processing")
                .fromSource("file", "accounts", Map.of(
                        "path", "/tmp/banking2/accounts.csv",
                        "format", "csv",
                        "options", Map.of("header", "true", "inferSchema", "true")
                ))
                // Validate account numbers
                .transform("validateAccount", Map.of(
                        "accountColumn", "account_number",
                        "validationColumn", "valid_account"
                ))
                // Calculate credit score based on banking behavior
                .transform("calculateCreditScore", Map.of(
                        "balanceColumn", "avg_balance",
                        "transactionCountColumn", "monthly_transactions",
                        "latePaymentColumn", "late_payments",
                        "accountAgeColumn", "account_age_months",
                        "scoreColumn", "credit_score",
                        "ratingColumn", "credit_rating"
                ))
                // Mask credit card numbers for security
                .transform("maskCreditCard", Map.of(
                        "column", "credit_card",
                        "maskChar", "*"
                ))
                // Filter out invalid accounts
                .transform("filter", Map.of(
                        "condition", "valid_account = true"
                ))
                .toSink("file", Map.of(
                        "path", "/tmp/output/banking2/customer_accounts.parquet",
                        "format", "parquet",
                        "mode", "overwrite"
                ))
                .build();

        PipelineEngine engine = new PipelineEngine(spark);
        engine.execute(config);
    }

    /**
     * Pipeline 2: Process transactions with fraud detection.
     */
    private static void processTransactionsWithFraudDetection(SparkSession spark) throws PipelineException {
        logger.info("=== Pipeline 2: Transaction Fraud Detection ===");

        PipelineConfig config = PipelineBuilder.create("fraud-detection")
                .fromSource("file", "transactions", Map.of(
                        "path", "/tmp/banking2/transactions.csv",
                        "format", "csv",
                        "options", Map.of("header", "true", "inferSchema", "true")
                ))
                // Detect fraudulent transactions
                .transform("detectFraud", Map.of(
                        "amountColumn", "amount",
                        "amountThreshold", 10000,
                        "fraudColumn", "is_fraud",
                        "scoreColumn", "fraud_score"
                ))
                // Add row number within customer partitions
                .transform("rowNumber", Map.of(
                        "partitionBy", List.of("customer_id"),
                        "orderBy", List.of("transaction_date"),
                        "alias", "transaction_seq"
                ))
                // Calculate running total per customer using window function
                .transform("window", Map.of(
                        "partitionBy", List.of("customer_id"),
                        "orderBy", List.of("transaction_date"),
                        "function", "sum",
                        "column", "amount",
                        "alias", "running_total"
                ))
                // Sort by fraud score (highest first)
                .transform("orderBy", Map.of(
                        "columns", List.of("fraud_score", "amount"),
                        "ascending", List.of(false, false)
                ))
                .toSink("file", Map.of(
                        "path", "/tmp/output/banking2/transactions_analyzed.parquet",
                        "format", "parquet",
                        "mode", "overwrite",
                        "partitionBy", List.of("is_fraud")
                ))
                .build();

        PipelineEngine engine = new PipelineEngine(spark);
        engine.execute(config);
    }

    /**
     * Pipeline 3: Process loan applications and calculate interest.
     */
    private static void processLoanApplications(SparkSession spark) throws PipelineException {
        logger.info("=== Pipeline 3: Loan Application Processing ===");

        PipelineConfig config = PipelineBuilder.create("loan-processing")
                .fromSource("file", "loans", Map.of(
                        "path", "/tmp/banking2/loan_applications.csv",
                        "format", "csv",
                        "options", Map.of("header", "true", "inferSchema", "true")
                ))
                .fromSource("file", "customers", Map.of(
                        "path", "/tmp/output/banking2/customer_accounts.parquet",
                        "format", "parquet"
                ))
                // Join with customer data
                .transform("innerJoin", Map.of(
                        "rightDataset", "customers",
                        "joinColumns", List.of("customer_id")
                ))
                // Calculate simple interest
                .transform("calculateInterest", Map.of(
                        "principalColumn", "loan_amount",
                        "rateColumn", "interest_rate",
                        "periodColumn", "tenure_years",
                        "periodUnit", "years",
                        "interestType", "simple",
                        "resultColumn", "total_interest"
                ))
                // Calculate total repayment amount
                .transform("withColumn", Map.of(
                        "columnName", "total_repayment",
                        "expression", "loan_amount + total_interest"
                ))
                // Calculate monthly EMI
                .transform("withColumn", Map.of(
                        "columnName", "monthly_emi",
                        "expression", "total_repayment / (tenure_years * 12)"
                ))
                // Add loan approval based on credit score
                .transform("withColumn", Map.of(
                        "columnName", "loan_approved",
                        "expression", "CASE " +
                                "WHEN credit_score >= 740 AND loan_amount <= 500000 THEN 'APPROVED' " +
                                "WHEN credit_score >= 670 AND loan_amount <= 200000 THEN 'APPROVED' " +
                                "WHEN credit_score >= 580 AND loan_amount <= 50000 THEN 'CONDITIONAL' " +
                                "ELSE 'REJECTED' END"
                ))
                .transform("select", Map.of(
                        "columns", List.of("customer_id", "account_number", "loan_amount",
                                "interest_rate", "tenure_years", "total_interest",
                                "total_repayment", "monthly_emi", "credit_score",
                                "credit_rating", "loan_approved")
                ))
                .toSink("file", Map.of(
                        "path", "/tmp/output/banking2/loan_decisions.parquet",
                        "format", "parquet",
                        "mode", "overwrite",
                        "partitionBy", List.of("loan_approved")
                ))
                .build();

        PipelineEngine engine = new PipelineEngine(spark);
        engine.execute(config);
    }

    /**
     * Pipeline 4: Generate customer rankings.
     */
    private static void generateCustomerRankings(SparkSession spark) throws PipelineException {
        logger.info("=== Pipeline 4: Customer Rankings ===");

        PipelineConfig config = PipelineBuilder.create("customer-rankings")
                .fromSource("file", Map.of(
                        "path", "/tmp/output/banking2/customer_accounts.parquet",
                        "format", "parquet"
                ))
                // Rank customers by credit score
                .transform("rank", Map.of(
                        "partitionBy", List.of("credit_rating"),
                        "orderBy", List.of("credit_score"),
                        "alias", "score_rank"
                ))
                // Rank customers by balance
                .transform("denseRank", Map.of(
                        "partitionBy", List.of("credit_rating"),
                        "orderBy", List.of("avg_balance"),
                        "alias", "balance_rank"
                ))
                // Filter top customers only
                .transform("filter", Map.of(
                        "condition", "score_rank <= 10"
                ))
                .transform("orderBy", Map.of(
                        "columns", List.of("credit_rating", "score_rank"),
                        "ascending", List.of(true, true)
                ))
                .toSink("file", Map.of(
                        "path", "/tmp/output/banking2/top_customers.parquet",
                        "format", "parquet",
                        "mode", "overwrite"
                ))
                .build();

        PipelineEngine engine = new PipelineEngine(spark);
        engine.execute(config);
    }

    /**
     * Display results from all pipelines.
     */
    private static void displayResults(SparkSession spark) {
        logger.info("\n" + "=".repeat(80));
        logger.info("PIPELINE RESULTS");
        logger.info("=".repeat(80));

        logger.info("\n=== Customer Accounts (with Credit Scores) ===");
        spark.read().parquet("/tmp/output/banking2/customer_accounts.parquet")
                .select("customer_id", "account_number", "credit_card", "credit_score",
                        "credit_rating", "avg_balance", "valid_account")
                .show(10, false);

        logger.info("\n=== Fraudulent Transactions ===");
        spark.read().parquet("/tmp/output/banking2/transactions_analyzed.parquet")
                .filter("is_fraud = true")
                .select("customer_id", "transaction_id", "amount", "fraud_score",
                        "transaction_seq", "running_total")
                .show(10, false);

        logger.info("\n=== Loan Decisions ===");
        spark.read().parquet("/tmp/output/banking2/loan_decisions.parquet")
                .select("customer_id", "loan_amount", "interest_rate", "total_interest",
                        "monthly_emi", "credit_score", "loan_approved")
                .show(10, false);

        logger.info("\n=== Top Customers by Credit Rating ===");
        spark.read().parquet("/tmp/output/banking2/top_customers.parquet")
                .select("customer_id", "credit_rating", "credit_score", "score_rank",
                        "avg_balance", "balance_rank")
                .show(20, false);

        logger.info("\n" + "=".repeat(80));
        logger.info("Pipeline completed successfully! ✓");
        logger.info("=".repeat(80) + "\n");
    }

    /**
     * Create sample banking data.
     */
    private static void createAdvancedBankingData(SparkSession spark) {
        logger.info("Creating sample banking data...");

        String accountsCSV = """
                customer_id,account_number,credit_card,avg_balance,monthly_transactions,late_payments,account_age_months
                C001,12345678,4532015112830366,25000,45,0,48
                C002,23456789,5425233430109903,75000,32,1,36
                C003,34567890,4916338506082832,150000,28,0,72
                C004,45678901,4024007134564842,5000,55,3,12
                C005,56789012,4485370538571359,200000,18,0,96
                C006,67890123,4539578763621486,35000,41,2,24
                C007,78901234,5425233430109904,90000,50,0,60
                C008,89012345,4916338506082833,12000,25,1,18
                C009,90123456,4024007134564843,180000,65,0,84
                C010,01234567,4485370538571360,8000,30,4,8
                """;

        String transactionsCSV = """
                transaction_id,customer_id,amount,transaction_date
                T001,C001,1500,2024-01-15
                T002,C002,15000,2024-01-15
                T003,C003,250,2024-01-16
                T004,C001,52000,2024-01-16
                T005,C004,3000,2024-01-17
                T006,C002,8500,2024-01-17
                T007,C005,20000,2024-01-18
                T008,C003,500,2024-01-18
                T009,C001,12000,2024-01-19
                T010,C006,45000,2024-01-19
                T011,C002,100000,2024-01-20
                T012,C007,750,2024-01-20
                T013,C003,3200,2024-01-21
                T014,C008,1500,2024-01-21
                T015,C004,50000,2024-01-22
                """;

        String loanApplicationsCSV = """
                loan_id,customer_id,loan_amount,interest_rate,tenure_years
                L001,C001,100000,8.5,5
                L002,C002,250000,7.5,10
                L003,C003,500000,6.5,15
                L004,C004,50000,12.0,3
                L005,C005,750000,6.0,20
                L006,C006,150000,9.0,7
                L007,C007,300000,7.0,10
                L008,C008,75000,11.0,5
                L009,C009,1000000,5.5,20
                L010,C010,25000,15.0,2
                """;

        try {
            java.nio.file.Files.createDirectories(java.nio.file.Path.of("/tmp/banking2"));
            java.nio.file.Files.writeString(
                    java.nio.file.Path.of("/tmp/banking2/accounts.csv"), accountsCSV);
            java.nio.file.Files.writeString(
                    java.nio.file.Path.of("/tmp/banking2/transactions.csv"), transactionsCSV);
            java.nio.file.Files.writeString(
                    java.nio.file.Path.of("/tmp/banking2/loan_applications.csv"), loanApplicationsCSV);
            logger.info("Sample data created successfully");
        } catch (Exception e) {
            logger.error("Failed to create sample data", e);
            throw new RuntimeException(e);
        }
    }
}
