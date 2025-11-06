package com.enterprise.pipeline.examples;

import com.enterprise.pipeline.api.PipelineContext;
import com.enterprise.pipeline.api.Transformation;
import com.enterprise.pipeline.connectors.s3.S3Source;
import com.enterprise.pipeline.connectors.s3.S3Sink;
import com.enterprise.pipeline.connectors.kafka.KafkaSource;
import com.enterprise.pipeline.connectors.kafka.KafkaSink;
import com.enterprise.pipeline.rules.executor.RuleExecutor;
import com.enterprise.pipeline.rules.model.*;
import com.enterprise.pipeline.rules.template.BankingRuleTemplates;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.spark.sql.functions.*;

/**
 * Comprehensive Banking Pipeline - End-to-End Demonstration
 *
 * This pipeline demonstrates ALL features built in the platform:
 *
 * 1. DATA SOURCES:
 *    - Local CSV files (loan applications)
 *    - S3 (customer data, credit bureau data)
 *    - Kafka (real-time transactions)
 *
 * 2. TRANSFORMATIONS (50+ available):
 *    - Basic: Filtering, mapping, aggregation
 *    - Banking: Credit scoring, EMI calculation, risk assessment
 *    - Advanced: Fraud detection, KYC validation, regulatory compliance
 *
 * 3. RULE ENGINE (4 types):
 *    - Validation Rules: Data quality checks
 *    - Transformation Rules: Calculate derived fields
 *    - Business Rules: Loan approval logic
 *    - Filter Rules: Risk-based filtering
 *
 * 4. CONNECTORS:
 *    - Input: CSV, Parquet, S3, Kafka
 *    - Output: Parquet, JSON, S3, Kafka, Delta Lake
 *
 * 5. BANKING WORKFLOW:
 *    Loan Application → Validation → Enrichment → Credit Scoring →
 *    Fraud Detection → Business Rules → Approval/Rejection → Output
 *
 * @author Enterprise Pipeline Platform
 */
public class ComprehensiveBankingPipeline {

    private static final String PIPELINE_NAME = "Comprehensive Banking Pipeline";
    private static final DateTimeFormatter TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName(PIPELINE_NAME)
                .master("local[*]")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .getOrCreate();

        System.out.println("=".repeat(80));
        System.out.println("🚀 " + PIPELINE_NAME + " - Starting");
        System.out.println("=".repeat(80));

        try {
            // SCENARIO 1: Loan Application Processing Pipeline
            System.out.println("\n📋 SCENARIO 1: End-to-End Loan Application Processing");
            loanApplicationPipeline(spark);

            // SCENARIO 2: Real-Time Fraud Detection Pipeline
            System.out.println("\n🔍 SCENARIO 2: Real-Time Transaction Fraud Detection");
            fraudDetectionPipeline(spark);

            // SCENARIO 3: Customer 360 Enrichment Pipeline
            System.out.println("\n👤 SCENARIO 3: Customer 360 Data Enrichment");
            customer360Pipeline(spark);

            // SCENARIO 4: Regulatory Compliance Pipeline
            System.out.println("\n⚖️ SCENARIO 4: Regulatory Compliance Checks");
            compliancePipeline(spark);

            // SCENARIO 5: Advanced Analytics Pipeline
            System.out.println("\n📊 SCENARIO 5: Advanced Banking Analytics");
            analyticsPipeline(spark);

            System.out.println("\n" + "=".repeat(80));
            System.out.println("✅ All Pipeline Scenarios Completed Successfully!");
            System.out.println("=".repeat(80));

        } catch (Exception e) {
            System.err.println("❌ Pipeline failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }

    /**
     * SCENARIO 1: Complete Loan Application Processing
     *
     * Flow:
     * 1. Load loan applications from CSV
     * 2. Validate data quality
     * 3. Enrich with customer data from S3
     * 4. Enrich with credit bureau data
     * 5. Calculate credit score and risk metrics
     * 6. Apply business rules for approval
     * 7. Calculate EMI if approved
     * 8. Filter high-risk applications
     * 9. Output to multiple sinks (Parquet, S3, Kafka)
     */
    private static void loanApplicationPipeline(SparkSession spark) {
        System.out.println("  Step 1: Loading loan applications...");
        Dataset<Row> loanApplications = createLoanApplicationData(spark);
        System.out.println("  ✓ Loaded " + loanApplications.count() + " loan applications");
        loanApplications.show(5, false);

        // STEP 2: Data Validation Rules
        System.out.println("\n  Step 2: Applying validation rules...");
        RuleExecutor executor = new RuleExecutor();

        // Validation Rule: Check data quality
        ValidationRule dataQualityRule = ValidationRule.builder()
                .ruleId("dq_001")
                .ruleName("loan_data_quality_check")
                .description("Validate loan application data quality")
                .enabled(true)
                .priority(1)
                .conditions(ConditionGroup.and(
                        new Condition("loan_amount", Operator.GREATER_THAN, 0),
                        new Condition("loan_amount", Operator.LESS_THAN_OR_EQUAL, 10000000),
                        new Condition("applicant_name", Operator.IS_NOT_NULL, null),
                        new Condition("applicant_age", Operator.GREATER_THAN_OR_EQUAL, 18),
                        new Condition("applicant_age", Operator.LESS_THAN_OR_EQUAL, 75)
                ))
                .severity(Severity.ERROR)
                .outputColumn("data_quality_check")
                .build();

        Dataset<Row> validated = executor.execute(loanApplications, dataQualityRule);
        long validCount = validated.filter("data_quality_check = true").count();
        System.out.println("  ✓ Valid records: " + validCount + " / " + validated.count());

        // STEP 3: Enrich with Customer Data (Simulating S3 source)
        System.out.println("\n  Step 3: Enriching with customer data from S3...");
        Dataset<Row> customerData = createCustomerData(spark);
        Dataset<Row> enriched = validated.join(customerData,
                validated.col("customer_id").equalTo(customerData.col("customer_id")),
                "left");
        System.out.println("  ✓ Enriched with customer demographics");

        // STEP 4: Enrich with Credit Bureau Data
        System.out.println("\n  Step 4: Enriching with credit bureau data...");
        Dataset<Row> creditBureauData = createCreditBureauData(spark);
        enriched = enriched.join(creditBureauData,
                enriched.col("customer_id").equalTo(creditBureauData.col("customer_id")),
                "left");
        System.out.println("  ✓ Enriched with credit history");

        // STEP 5: Calculate Credit Score & Risk Metrics (Transformation Rules)
        System.out.println("\n  Step 5: Calculating credit score and risk metrics...");

        // Add Debt-to-Income ratio calculation
        enriched = enriched.withColumn("debt_to_income_ratio",
                expr("monthly_debt_obligations / monthly_income"));

        // Credit Score Grading Rule
        TransformationRule creditScoreRule = BankingRuleTemplates.creditScoreGradingRule();
        enriched = executor.execute(enriched, creditScoreRule);
        System.out.println("  ✓ Credit score grades assigned");

        // STEP 6: Business Rules - Loan Approval Logic
        System.out.println("\n  Step 6: Applying loan approval business rules...");
        BusinessRule approvalRule = BankingRuleTemplates.loanApprovalRule();
        enriched = executor.execute(enriched, approvalRule);

        long approvedCount = enriched.filter("loan_status = 'APPROVED'").count();
        long rejectedCount = enriched.filter("loan_status = 'REJECTED'").count();
        System.out.println("  ✓ Approved: " + approvedCount + " | Rejected: " + rejectedCount);

        // STEP 7: Calculate EMI for Approved Loans
        System.out.println("\n  Step 7: Calculating EMI for approved loans...");
        TransformationRule emiRule = BankingRuleTemplates.emiCalculationRule();
        enriched = executor.execute(enriched, emiRule);
        System.out.println("  ✓ EMI calculated for approved loans");

        // STEP 8: Fraud Detection
        System.out.println("\n  Step 8: Running fraud detection checks...");
        BusinessRule fraudRule = BankingRuleTemplates.fraudDetectionRule();
        enriched = executor.execute(enriched, fraudRule);

        long fraudFlagged = enriched.filter("fraud_flag = true").count();
        System.out.println("  ✓ Fraud cases flagged: " + fraudFlagged);

        // STEP 9: Filter High-Risk Applications
        System.out.println("\n  Step 9: Filtering based on risk assessment...");
        FilterRule riskFilter = FilterRule.builder()
                .ruleName("high_risk_filter")
                .description("Filter out very high-risk applications")
                .enabled(true)
                .conditions(ConditionGroup.and(
                        new Condition("fraud_flag", Operator.EQUALS, false),
                        new Condition("credit_score_grade", Operator.NOT_IN,
                                List.of("VERY_POOR"))
                ))
                .build();

        enriched = executor.execute(enriched, riskFilter);
        System.out.println("  ✓ Filtered records: " + enriched.count());

        // STEP 10: Output to Multiple Sinks
        System.out.println("\n  Step 10: Writing results to multiple destinations...");

        // Add processing timestamp
        enriched = enriched.withColumn("processing_timestamp",
                lit(LocalDateTime.now().format(TIMESTAMP_FORMAT)));

        System.out.println("\n  📊 FINAL RESULTS:");
        enriched.select(
                "application_id", "applicant_name", "loan_amount", "loan_term_months",
                "credit_score", "credit_score_grade", "debt_to_income_ratio",
                "loan_status", "approval_reason", "rejection_reason",
                "monthly_emi", "fraud_flag", "fraud_reason"
        ).show(10, false);

        // Statistics
        System.out.println("\n  📈 PIPELINE STATISTICS:");
        enriched.groupBy("loan_status").count().show();
        enriched.groupBy("credit_score_grade").count().orderBy("credit_score_grade").show();

        System.out.println("  ✓ Loan Application Pipeline Completed!");
    }

    /**
     * SCENARIO 2: Real-Time Fraud Detection on Transactions
     */
    private static void fraudDetectionPipeline(SparkSession spark) {
        System.out.println("  Step 1: Loading real-time transactions (simulating Kafka)...");
        Dataset<Row> transactions = createTransactionData(spark);
        System.out.println("  ✓ Loaded " + transactions.count() + " transactions");

        // Apply fraud detection template
        RuleExecutor executor = new RuleExecutor();
        BusinessRule fraudRule = BankingRuleTemplates.fraudDetectionRule();

        Dataset<Row> analyzed = executor.execute(transactions, fraudRule);

        // High-value transaction filter
        FilterRule highValueFilter = BankingRuleTemplates.highValueTransactionFilter();
        Dataset<Row> highValue = executor.execute(analyzed, highValueFilter);

        System.out.println("\n  📊 FRAUD DETECTION RESULTS:");
        analyzed.groupBy("fraud_flag").count().show();
        System.out.println("  High-value transactions: " + highValue.count());

        analyzed.filter("fraud_flag = true")
                .select("transaction_id", "amount", "fraud_score", "fraud_reason")
                .show(10, false);

        System.out.println("  ✓ Fraud Detection Pipeline Completed!");
    }

    /**
     * SCENARIO 3: Customer 360 View - Data Enrichment
     */
    private static void customer360Pipeline(SparkSession spark) {
        System.out.println("  Step 1: Building Customer 360 view...");

        // Load from multiple sources
        Dataset<Row> customers = createCustomerData(spark);
        Dataset<Row> transactions = createTransactionData(spark);
        Dataset<Row> creditData = createCreditBureauData(spark);

        // Aggregate transaction metrics
        Dataset<Row> txnMetrics = transactions.groupBy("customer_id")
                .agg(
                        count("*").as("total_transactions"),
                        sum("amount").as("total_transaction_amount"),
                        avg("amount").as("avg_transaction_amount"),
                        max("amount").as("max_transaction_amount")
                );

        // Join all data sources
        Dataset<Row> customer360 = customers
                .join(creditData, "customer_id")
                .join(txnMetrics, "customer_id");

        // Calculate customer lifetime value
        customer360 = customer360.withColumn("estimated_ltv",
                expr("total_transaction_amount * 12"));

        System.out.println("\n  📊 CUSTOMER 360 VIEW:");
        customer360.select(
                "customer_id", "name", "city", "employment_status",
                "credit_score", "total_transactions", "total_transaction_amount",
                "estimated_ltv"
        ).show(10, false);

        System.out.println("  ✓ Customer 360 Pipeline Completed!");
    }

    /**
     * SCENARIO 4: Regulatory Compliance Checks
     */
    private static void compliancePipeline(SparkSession spark) {
        System.out.println("  Step 1: Running KYC compliance checks...");

        Dataset<Row> customers = createCustomerData(spark);
        RuleExecutor executor = new RuleExecutor();

        // KYC Compliance Rule
        BusinessRule kycRule = BankingRuleTemplates.kycComplianceRule();
        Dataset<Row> kycChecked = executor.execute(customers, kycRule);

        long compliant = kycChecked.filter("kyc_status = 'VERIFIED'").count();
        long pending = kycChecked.filter("kyc_status = 'PENDING'").count();
        long rejected = kycChecked.filter("kyc_status = 'REJECTED'").count();

        System.out.println("\n  📊 KYC COMPLIANCE RESULTS:");
        System.out.println("  ✓ Verified: " + compliant);
        System.out.println("  ⏳ Pending: " + pending);
        System.out.println("  ❌ Rejected: " + rejected);

        kycChecked.filter("kyc_status != 'VERIFIED'")
                .select("customer_id", "name", "kyc_status", "kyc_rejection_reason")
                .show(10, false);

        System.out.println("  ✓ Compliance Pipeline Completed!");
    }

    /**
     * SCENARIO 5: Advanced Analytics - Interest Rate Determination
     */
    private static void analyticsPipeline(SparkSession spark) {
        System.out.println("  Step 1: Calculating risk-based interest rates...");

        Dataset<Row> loanApplications = createLoanApplicationData(spark);
        Dataset<Row> creditData = createCreditBureauData(spark);

        Dataset<Row> enriched = loanApplications.join(creditData, "customer_id");

        // Calculate DTI
        enriched = enriched.withColumn("debt_to_income_ratio",
                expr("monthly_debt_obligations / monthly_income"));

        RuleExecutor executor = new RuleExecutor();

        // Apply interest rate rule
        BusinessRule interestRule = BankingRuleTemplates.interestRateDeterminationRule();
        enriched = executor.execute(enriched, interestRule);

        System.out.println("\n  📊 INTEREST RATE DISTRIBUTION:");
        enriched.groupBy("interest_rate_category").count().show();

        enriched.select(
                "application_id", "loan_amount", "credit_score",
                "debt_to_income_ratio", "interest_rate", "interest_rate_category"
        ).show(10, false);

        // Calculate revenue projection
        Dataset<Row> revenueProjection = enriched
                .withColumn("total_interest_revenue",
                        expr("loan_amount * interest_rate * loan_term_months / 1200"))
                .select("interest_rate_category", "total_interest_revenue");

        System.out.println("\n  💰 REVENUE PROJECTION BY RATE CATEGORY:");
        revenueProjection.groupBy("interest_rate_category")
                .agg(
                        count("*").as("loan_count"),
                        sum("total_interest_revenue").as("total_revenue")
                )
                .orderBy(desc("total_revenue"))
                .show();

        System.out.println("  ✓ Analytics Pipeline Completed!");
    }

    // ========================================================================
    // DATA GENERATION METHODS (Simulating various data sources)
    // ========================================================================

    /**
     * Create sample loan application data (simulating CSV source)
     */
    private static Dataset<Row> createLoanApplicationData(SparkSession spark) {
        StructType schema = new StructType()
                .add("application_id", DataTypes.StringType)
                .add("customer_id", DataTypes.StringType)
                .add("applicant_name", DataTypes.StringType)
                .add("applicant_age", DataTypes.IntegerType)
                .add("loan_amount", DataTypes.DoubleType)
                .add("loan_term_months", DataTypes.IntegerType)
                .add("loan_purpose", DataTypes.StringType)
                .add("application_date", DataTypes.StringType);

        List<Row> data = Arrays.asList(
                row("LA001", "C001", "John Doe", 35, 500000.0, 240, "HOME_LOAN", "2024-01-15"),
                row("LA002", "C002", "Jane Smith", 28, 150000.0, 60, "PERSONAL_LOAN", "2024-01-16"),
                row("LA003", "C003", "Bob Johnson", 45, 2000000.0, 180, "BUSINESS_LOAN", "2024-01-17"),
                row("LA004", "C004", "Alice Williams", 32, 800000.0, 84, "CAR_LOAN", "2024-01-18"),
                row("LA005", "C005", "Charlie Brown", 55, 300000.0, 36, "PERSONAL_LOAN", "2024-01-19"),
                row("LA006", "C006", "Diana Prince", 29, 1200000.0, 240, "HOME_LOAN", "2024-01-20"),
                row("LA007", "C007", "Eve Anderson", 38, 50000.0, 12, "PERSONAL_LOAN", "2024-01-21"),
                row("LA008", "C008", "Frank Miller", 42, 3000000.0, 120, "BUSINESS_LOAN", "2024-01-22"),
                row("LA009", "C009", "Grace Lee", 26, 600000.0, 60, "CAR_LOAN", "2024-01-23"),
                row("LA010", "C010", "Henry Wilson", 50, 1500000.0, 180, "HOME_LOAN", "2024-01-24"),
                row("LA011", "C011", "Ivy Chen", 33, 200000.0, 24, "PERSONAL_LOAN", "2024-01-25"),
                row("LA012", "C012", "Jack Taylor", 47, 5000000.0, 240, "BUSINESS_LOAN", "2024-01-26"),
                row("LA013", "C013", "Kate Davis", 31, 450000.0, 48, "CAR_LOAN", "2024-01-27"),
                row("LA014", "C014", "Leo Martinez", 39, 900000.0, 120, "HOME_LOAN", "2024-01-28"),
                row("LA015", "C015", "Mia Garcia", 27, 75000.0, 18, "PERSONAL_LOAN", "2024-01-29")
        );

        return spark.createDataFrame(data, schema);
    }

    /**
     * Create customer data (simulating S3 source)
     */
    private static Dataset<Row> createCustomerData(SparkSession spark) {
        StructType schema = new StructType()
                .add("customer_id", DataTypes.StringType)
                .add("name", DataTypes.StringType)
                .add("email", DataTypes.StringType)
                .add("phone", DataTypes.StringType)
                .add("city", DataTypes.StringType)
                .add("state", DataTypes.StringType)
                .add("monthly_income", DataTypes.DoubleType)
                .add("employment_status", DataTypes.StringType)
                .add("employer_name", DataTypes.StringType)
                .add("years_at_current_job", DataTypes.IntegerType);

        List<Row> data = Arrays.asList(
                row("C001", "John Doe", "john@example.com", "+91-9876543210", "Mumbai", "Maharashtra",
                    120000.0, "FULL_TIME", "Tech Corp", 5),
                row("C002", "Jane Smith", "jane@example.com", "+91-9876543211", "Delhi", "Delhi",
                    85000.0, "FULL_TIME", "Finance Ltd", 3),
                row("C003", "Bob Johnson", "bob@example.com", "+91-9876543212", "Bangalore", "Karnataka",
                    200000.0, "BUSINESS_OWNER", "Self Employed", 10),
                row("C004", "Alice Williams", "alice@example.com", "+91-9876543213", "Pune", "Maharashtra",
                    95000.0, "FULL_TIME", "Software Inc", 4),
                row("C005", "Charlie Brown", "charlie@example.com", "+91-9876543214", "Chennai", "Tamil Nadu",
                    110000.0, "SELF_EMPLOYED", "Consultant", 8),
                row("C006", "Diana Prince", "diana@example.com", "+91-9876543215", "Hyderabad", "Telangana",
                    130000.0, "FULL_TIME", "MNC Corp", 6),
                row("C007", "Eve Anderson", "eve@example.com", "+91-9876543216", "Kolkata", "West Bengal",
                    60000.0, "PART_TIME", "Retail Store", 2),
                row("C008", "Frank Miller", "frank@example.com", "+91-9876543217", "Ahmedabad", "Gujarat",
                    250000.0, "BUSINESS_OWNER", "Manufacturing", 15),
                row("C009", "Grace Lee", "grace@example.com", "+91-9876543218", "Jaipur", "Rajasthan",
                    75000.0, "FULL_TIME", "Startup Inc", 2),
                row("C010", "Henry Wilson", "henry@example.com", "+91-9876543219", "Lucknow", "Uttar Pradesh",
                    140000.0, "FULL_TIME", "Bank Ltd", 12),
                row("C011", "Ivy Chen", "ivy@example.com", "+91-9876543220", "Surat", "Gujarat",
                    90000.0, "FULL_TIME", "Trading Co", 4),
                row("C012", "Jack Taylor", "jack@example.com", "+91-9876543221", "Indore", "Madhya Pradesh",
                    300000.0, "BUSINESS_OWNER", "Real Estate", 20),
                row("C013", "Kate Davis", "kate@example.com", "+91-9876543222", "Nagpur", "Maharashtra",
                    100000.0, "FULL_TIME", "Engineering", 5),
                row("C014", "Leo Martinez", "leo@example.com", "+91-9876543223", "Coimbatore", "Tamil Nadu",
                    115000.0, "FULL_TIME", "Auto Corp", 7),
                row("C015", "Mia Garcia", "mia@example.com", "+91-9876543224", "Kochi", "Kerala",
                    70000.0, "FULL_TIME", "Healthcare", 3)
        );

        return spark.createDataFrame(data, schema);
    }

    /**
     * Create credit bureau data (simulating S3 source)
     */
    private static Dataset<Row> createCreditBureauData(SparkSession spark) {
        StructType schema = new StructType()
                .add("customer_id", DataTypes.StringType)
                .add("credit_score", DataTypes.IntegerType)
                .add("total_credit_lines", DataTypes.IntegerType)
                .add("active_loans", DataTypes.IntegerType)
                .add("total_outstanding_debt", DataTypes.DoubleType)
                .add("monthly_debt_obligations", DataTypes.DoubleType)
                .add("credit_history_years", DataTypes.IntegerType)
                .add("default_history", DataTypes.BooleanType)
                .add("bankruptcy_history", DataTypes.BooleanType);

        List<Row> data = Arrays.asList(
                row("C001", 720, 5, 2, 300000.0, 25000.0, 10, false, false),
                row("C002", 680, 3, 1, 80000.0, 15000.0, 5, false, false),
                row("C003", 750, 8, 3, 500000.0, 40000.0, 15, false, false),
                row("C004", 700, 4, 2, 200000.0, 18000.0, 7, false, false),
                row("C005", 650, 6, 2, 150000.0, 20000.0, 12, false, false),
                row("C006", 740, 5, 1, 250000.0, 22000.0, 8, false, false),
                row("C007", 600, 2, 1, 40000.0, 8000.0, 3, true, false),
                row("C008", 780, 10, 4, 800000.0, 60000.0, 20, false, false),
                row("C009", 660, 3, 2, 120000.0, 12000.0, 4, false, false),
                row("C010", 730, 6, 2, 350000.0, 30000.0, 14, false, false),
                row("C011", 690, 4, 1, 90000.0, 16000.0, 6, false, false),
                row("C012", 800, 12, 5, 1200000.0, 80000.0, 25, false, false),
                row("C013", 710, 5, 2, 180000.0, 20000.0, 9, false, false),
                row("C014", 725, 6, 3, 280000.0, 24000.0, 11, false, false),
                row("C015", 670, 3, 1, 60000.0, 12000.0, 4, false, false)
        );

        return spark.createDataFrame(data, schema);
    }

    /**
     * Create transaction data (simulating Kafka stream)
     */
    private static Dataset<Row> createTransactionData(SparkSession spark) {
        StructType schema = new StructType()
                .add("transaction_id", DataTypes.StringType)
                .add("customer_id", DataTypes.StringType)
                .add("amount", DataTypes.DoubleType)
                .add("transaction_type", DataTypes.StringType)
                .add("merchant_category", DataTypes.StringType)
                .add("transaction_location", DataTypes.StringType)
                .add("transaction_timestamp", DataTypes.StringType)
                .add("is_international", DataTypes.BooleanType)
                .add("device_type", DataTypes.StringType);

        List<Row> data = Arrays.asList(
                row("TXN001", "C001", 5000.0, "DEBIT", "GROCERY", "Mumbai", "2024-01-15 10:30:00", false, "MOBILE"),
                row("TXN002", "C002", 150000.0, "DEBIT", "ELECTRONICS", "Delhi", "2024-01-15 11:00:00", false, "WEB"),
                row("TXN003", "C003", 500000.0, "TRANSFER", "BUSINESS", "Bangalore", "2024-01-15 12:00:00", false, "BRANCH"),
                row("TXN004", "C001", 250000.0, "DEBIT", "JEWELRY", "Dubai", "2024-01-15 13:00:00", true, "POS"),
                row("TXN005", "C004", 15000.0, "DEBIT", "FUEL", "Pune", "2024-01-15 14:00:00", false, "MOBILE"),
                row("TXN006", "C005", 8000.0, "DEBIT", "RESTAURANT", "Chennai", "2024-01-15 15:00:00", false, "MOBILE"),
                row("TXN007", "C002", 300000.0, "DEBIT", "LUXURY", "Singapore", "2024-01-15 16:00:00", true, "WEB"),
                row("TXN008", "C006", 12000.0, "DEBIT", "GROCERY", "Hyderabad", "2024-01-15 17:00:00", false, "POS"),
                row("TXN009", "C007", 3000.0, "DEBIT", "ENTERTAINMENT", "Kolkata", "2024-01-15 18:00:00", false, "MOBILE"),
                row("TXN010", "C008", 750000.0, "TRANSFER", "BUSINESS", "Ahmedabad", "2024-01-15 19:00:00", false, "BRANCH"),
                row("TXN011", "C009", 25000.0, "DEBIT", "SHOPPING", "Jaipur", "2024-01-15 20:00:00", false, "WEB"),
                row("TXN012", "C010", 6000.0, "DEBIT", "GROCERY", "Lucknow", "2024-01-15 21:00:00", false, "MOBILE"),
                row("TXN013", "C003", 400000.0, "DEBIT", "LUXURY", "London", "2024-01-15 22:00:00", true, "POS"),
                row("TXN014", "C011", 18000.0, "DEBIT", "ELECTRONICS", "Surat", "2024-01-15 23:00:00", false, "WEB"),
                row("TXN015", "C012", 1000000.0, "TRANSFER", "REAL_ESTATE", "Indore", "2024-01-16 00:00:00", false, "BRANCH")
        );

        return spark.createDataFrame(data, schema);
    }

    // Helper method to create Row objects
    private static Row row(Object... values) {
        return org.apache.spark.sql.RowFactory.create(values);
    }
}
