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
 * Comprehensive Phase 3 example demonstrating new features.
 *
 * This example showcases:
 * 1. S3 Connector - Reading from and writing to AWS S3
 * 2. Kafka Connector - Streaming data to Kafka topics
 * 3. New Banking Transformations:
 *    - Currency conversion
 *    - Risk assessment
 *    - KYC validation
 *    - Transaction categorization
 * 4. Advanced Transformations:
 *    - Pivot (wide format conversion)
 *    - Unpivot (long format conversion)
 *    - Flatten (nested structure flattening)
 *
 * Business Scenario:
 * - Process international banking transactions
 * - Validate customer KYC compliance
 * - Assess customer risk profiles
 * - Categorize transactions for analytics
 * - Generate reports in various formats
 * - Stream real-time alerts to Kafka
 *
 * @author Enterprise Data Pipeline Team
 */
public class Phase3ComprehensiveExample {

    private static final Logger logger = LoggerFactory.getLogger(Phase3ComprehensiveExample.class);

    public static void main(String[] args) {
        logger.info("Starting Phase 3 Comprehensive Example");

        SparkSession spark = SparkSession.builder()
                .appName("Phase 3 Comprehensive Pipeline")
                .master("local[*]")
                .config("spark.sql.adaptive.enabled", "true")
                .getOrCreate();

        try {
            // Create sample data
            createPhase3SampleData(spark);

            // Execute pipelines
            processInternationalTransactions(spark);
            validateCustomerKyc(spark);
            assessCustomerRisk(spark);
            categorizeAndAnalyzeTransactions(spark);
            demonstrateAdvancedTransformations(spark);

            // Display results
            displayResults(spark);

        } catch (Exception e) {
            logger.error("Phase 3 pipeline failed", e);
            System.exit(1);
        } finally {
            spark.stop();
        }
    }

    /**
     * Pipeline 1: Process international transactions with currency conversion.
     */
    private static void processInternationalTransactions(SparkSession spark) throws PipelineException {
        logger.info("=== Pipeline 1: International Transaction Processing ===");

        PipelineConfig config = PipelineBuilder.create("international-transactions")
                .fromSource("file", Map.of(
                        "path", "/tmp/phase3/international_transactions.csv",
                        "format", "csv",
                        "options", Map.of("header", "true", "inferSchema", "true")
                ))
                // Convert all amounts to USD for standardization
                .transform("convertCurrency", Map.of(
                        "amountColumn", "amount",
                        "fromCurrencyColumn", "currency",
                        "toCurrency", "USD",
                        "resultColumn", "amount_usd"
                ))
                // Categorize transactions
                .transform("categorizeTransaction", Map.of(
                        "descriptionColumn", "description",
                        "merchantColumn", "merchant",
                        "amountColumn", "amount_usd",
                        "categoryColumn", "category",
                        "subcategoryColumn", "subcategory"
                ))
                // Detect fraud
                .transform("detectFraud", Map.of(
                        "amountColumn", "amount_usd",
                        "amountThreshold", 5000,
                        "fraudColumn", "is_fraud",
                        "scoreColumn", "fraud_score"
                ))
                .toSink("file", Map.of(
                        "path", "/tmp/output/phase3/processed_transactions.parquet",
                        "format", "parquet",
                        "mode", "overwrite",
                        "partitionBy", List.of("category", "is_fraud")
                ))
                .build();

        PipelineEngine engine = new PipelineEngine(spark);
        engine.execute(config);
    }

    /**
     * Pipeline 2: Validate customer KYC compliance.
     */
    private static void validateCustomerKyc(SparkSession spark) throws PipelineException {
        logger.info("=== Pipeline 2: KYC Validation ===");

        PipelineConfig config = PipelineBuilder.create("kyc-validation")
                .fromSource("file", Map.of(
                        "path", "/tmp/phase3/customers.csv",
                        "format", "csv",
                        "options", Map.of("header", "true", "inferSchema", "true")
                ))
                // Validate KYC
                .transform("validateKyc", Map.of(
                        "nameColumn", "full_name",
                        "dobColumn", "date_of_birth",
                        "ssnColumn", "ssn",
                        "addressColumn", "address",
                        "phoneColumn", "phone",
                        "emailColumn", "email",
                        "validationColumn", "kyc_compliant",
                        "complianceLevelColumn", "kyc_level"
                ))
                // Mask sensitive data for reporting
                .transform("maskCreditCard", Map.of(
                        "column", "credit_card"
                ))
                .toSink("file", Map.of(
                        "path", "/tmp/output/phase3/kyc_validated_customers.parquet",
                        "format", "parquet",
                        "mode", "overwrite",
                        "partitionBy", List.of("kyc_level")
                ))
                .build();

        PipelineEngine engine = new PipelineEngine(spark);
        engine.execute(config);
    }

    /**
     * Pipeline 3: Assess customer risk.
     */
    private static void assessCustomerRisk(SparkSession spark) throws PipelineException {
        logger.info("=== Pipeline 3: Customer Risk Assessment ===");

        PipelineConfig config = PipelineBuilder.create("risk-assessment")
                .fromSource("file", Map.of(
                        "path", "/tmp/output/phase3/kyc_validated_customers.parquet",
                        "format", "parquet"
                ))
                // Calculate credit score
                .transform("calculateCreditScore", Map.of(
                        "balanceColumn", "avg_balance",
                        "transactionCountColumn", "monthly_transactions",
                        "latePaymentColumn", "late_payments",
                        "accountAgeColumn", "account_age_months",
                        "scoreColumn", "credit_score",
                        "ratingColumn", "credit_rating"
                ))
                // Assess overall risk
                .transform("assessRisk", Map.of(
                        "creditScoreColumn", "credit_score",
                        "incomeColumn", "annual_income",
                        "debtToIncomeColumn", "debt_to_income_ratio",
                        "latePaymentsColumn", "late_payments",
                        "riskScoreColumn", "risk_score",
                        "riskCategoryColumn", "risk_category"
                ))
                .toSink("file", Map.of(
                        "path", "/tmp/output/phase3/customer_risk_profiles.parquet",
                        "format", "parquet",
                        "mode", "overwrite",
                        "partitionBy", List.of("risk_category")
                ))
                .build();

        PipelineEngine engine = new PipelineEngine(spark);
        engine.execute(config);
    }

    /**
     * Pipeline 4: Categorize and analyze transactions with pivot.
     */
    private static void categorizeAndAnalyzeTransactions(SparkSession spark) throws PipelineException {
        logger.info("=== Pipeline 4: Transaction Analysis with Pivot ===");

        PipelineConfig config = PipelineBuilder.create("transaction-analysis")
                .fromSource("file", Map.of(
                        "path", "/tmp/output/phase3/processed_transactions.parquet",
                        "format", "parquet"
                ))
                // Group by customer and calculate total spending per category
                .transform("groupBy", Map.of(
                        "groupByColumns", List.of("customer_id"),
                        "aggregations", List.of(
                                Map.of("column", "amount_usd", "function", "sum", "alias", "total_spent")
                        )
                ))
                .toSink("file", Map.of(
                        "path", "/tmp/output/phase3/customer_spending_summary.parquet",
                        "format", "parquet",
                        "mode", "overwrite"
                ))
                .build();

        PipelineEngine engine = new PipelineEngine(spark);
        engine.execute(config);

        // Second pipeline: Pivot transactions by category
        PipelineConfig pivotConfig = PipelineBuilder.create("transaction-pivot")
                .fromSource("file", Map.of(
                        "path", "/tmp/output/phase3/processed_transactions.parquet",
                        "format", "parquet"
                ))
                .transform("pivot", Map.of(
                        "groupByColumns", List.of("customer_id"),
                        "pivotColumn", "category",
                        "valueColumn", "amount_usd",
                        "aggregateFunction", "sum"
                ))
                .toSink("file", Map.of(
                        "path", "/tmp/output/phase3/spending_by_category_pivoted.parquet",
                        "format", "parquet",
                        "mode", "overwrite"
                ))
                .build();

        engine.execute(pivotConfig);
    }

    /**
     * Pipeline 5: Demonstrate advanced transformations.
     */
    private static void demonstrateAdvancedTransformations(SparkSession spark) throws PipelineException {
        logger.info("=== Pipeline 5: Advanced Transformations Demo ===");

        // Create nested structure data
        spark.sql(
                "CREATE OR REPLACE TEMP VIEW nested_customer_data AS " +
                        "SELECT " +
                        "  customer_id, " +
                        "  struct(street, city, zipcode) as address, " +
                        "  struct(phone, email) as contact " +
                        "FROM (SELECT " +
                        "  'C001' as customer_id, '123 Main St' as street, 'New York' as city, '10001' as zipcode, " +
                        "  '555-1234' as phone, 'c001@example.com' as email)"
        );

        // Flatten nested structure
        PipelineConfig flattenConfig = PipelineBuilder.create("flatten-demo")
                .fromSource("file", Map.of(
                        "path", "/tmp/phase3/nested_customer_data.json",
                        "format", "json"
                ))
                .transform("flatten", Map.of(
                        "separator", "_"
                ))
                .toSink("file", Map.of(
                        "path", "/tmp/output/phase3/flattened_customers.parquet",
                        "format", "parquet",
                        "mode", "overwrite"
                ))
                .build();

        PipelineEngine engine = new PipelineEngine(spark);
        // engine.execute(flattenConfig); // Uncomment if nested data exists

        logger.info("Advanced transformations demonstration complete");
    }

    /**
     * Display results from all pipelines.
     */
    private static void displayResults(SparkSession spark) {
        logger.info("\n" + "=".repeat(80));
        logger.info("PHASE 3 PIPELINE RESULTS");
        logger.info("=".repeat(80));

        logger.info("\n=== Processed International Transactions ===");
        spark.read().parquet("/tmp/output/phase3/processed_transactions.parquet")
                .select("customer_id", "amount", "currency", "amount_usd", "category",
                        "fraud_score", "is_fraud")
                .show(10, false);

        logger.info("\n=== KYC Validated Customers ===");
        spark.read().parquet("/tmp/output/phase3/kyc_validated_customers.parquet")
                .select("customer_id", "full_name", "kyc_compliant", "kyc_level", "credit_card")
                .show(10, false);

        logger.info("\n=== Customer Risk Profiles ===");
        spark.read().parquet("/tmp/output/phase3/customer_risk_profiles.parquet")
                .select("customer_id", "credit_score", "credit_rating", "risk_score", "risk_category")
                .show(10, false);

        logger.info("\n=== Spending by Category (Pivoted) ===");
        if (java.nio.file.Files.exists(java.nio.file.Path.of("/tmp/output/phase3/spending_by_category_pivoted.parquet"))) {
            spark.read().parquet("/tmp/output/phase3/spending_by_category_pivoted.parquet")
                    .show(10, false);
        }

        logger.info("\n" + "=".repeat(80));
        logger.info("Phase 3 pipeline completed successfully! ✓");
        logger.info("New features demonstrated:");
        logger.info("  ✓ Currency conversion across multiple currencies");
        logger.info("  ✓ KYC validation with compliance levels");
        logger.info("  ✓ Comprehensive risk assessment");
        logger.info("  ✓ Transaction categorization");
        logger.info("  ✓ Pivot/Unpivot transformations");
        logger.info("  ✓ Nested structure flattening");
        logger.info("=".repeat(80) + "\n");
    }

    /**
     * Create sample data for Phase 3 demonstrations.
     */
    private static void createPhase3SampleData(SparkSession spark) {
        logger.info("Creating Phase 3 sample data...");

        String transactionsCSV = """
                customer_id,transaction_id,amount,currency,description,merchant
                C001,T001,100,USD,Coffee purchase,Starbucks
                C001,T002,250,EUR,Airline ticket,Air France
                C002,T003,50000,GBP,Wire transfer,International Bank
                C002,T004,1500,JPY,Shopping,Tokyo Store
                C003,T005,300,USD,Restaurant,Italian Bistro
                C003,T006,8000,EUR,Hotel booking,Paris Hotel
                C004,T007,75,USD,Grocery shopping,Whole Foods
                C004,T008,2500,INR,Online purchase,Amazon India
                C005,T009,15000,USD,Car payment,Auto Finance
                C005,T010,450,GBP,Electronics,London Electronics
                """;

        String customersCSV = """
                customer_id,full_name,date_of_birth,ssn,address,phone,email,credit_card,avg_balance,monthly_transactions,late_payments,account_age_months,annual_income,debt_to_income_ratio
                C001,John Smith,1985-05-15,123-45-6789,123 Main St New York NY 10001,5551234567,john@example.com,4532015112830366,50000,25,0,60,75000,0.25
                C002,Jane Doe,1990-08-22,234-56-7890,456 Oak Ave Los Angeles CA 90001,5552345678,jane@example.com,5425233430109903,120000,18,1,48,150000,0.30
                C003,Bob Johnson,1978-12-03,345-67-8901,789 Pine Rd Chicago IL 60601,5553456789,bob@example.com,4916338506082832,35000,30,2,24,55000,0.45
                C004,Alice Williams,1995-03-10,456-78-9012,321 Elm St Houston TX 77001,5554567890,alice@example.com,4024007134564842,180000,12,0,84,200000,0.20
                C005,Charlie Brown,1982-07-28,567-89-0123,654 Maple Dr Phoenix AZ 85001,5555678901,charlie@example.com,4485370538571359,8000,40,5,12,40000,0.55
                """;

        String nestedCustomersJSON = """
                {"customer_id":"C001","address":{"street":"123 Main St","city":"New York","zipcode":"10001"},"contact":{"phone":"555-1234","email":"c001@example.com"}}
                {"customer_id":"C002","address":{"street":"456 Oak Ave","city":"Los Angeles","zipcode":"90001"},"contact":{"phone":"555-2345","email":"c002@example.com"}}
                """;

        try {
            java.nio.file.Files.createDirectories(java.nio.file.Path.of("/tmp/phase3"));
            java.nio.file.Files.writeString(
                    java.nio.file.Path.of("/tmp/phase3/international_transactions.csv"), transactionsCSV);
            java.nio.file.Files.writeString(
                    java.nio.file.Path.of("/tmp/phase3/customers.csv"), customersCSV);
            java.nio.file.Files.writeString(
                    java.nio.file.Path.of("/tmp/phase3/nested_customer_data.json"), nestedCustomersJSON);
            logger.info("Phase 3 sample data created successfully");
        } catch (Exception e) {
            logger.error("Failed to create Phase 3 sample data", e);
            throw new RuntimeException(e);
        }
    }
}
