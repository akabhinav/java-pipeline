package com.enterprise.pipeline.examples;

import com.enterprise.pipeline.rules.executor.RuleExecutor;
import com.enterprise.pipeline.rules.model.*;
import com.enterprise.pipeline.rules.template.BankingRuleTemplates;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Example demonstrating the UI-based Rule Engine.
 *
 * Shows how to:
 * 1. Use pre-built banking templates
 * 2. Create custom rules programmatically
 * 3. Execute rules on datasets
 * 4. Chain multiple rules together
 *
 * @author Enterprise Data Pipeline Team
 */
public class RuleEngineExample {

    private static final Logger logger = LoggerFactory.getLogger(RuleEngineExample.class);

    public static void main(String[] args) {
        logger.info("Starting Rule Engine Example");

        SparkSession spark = SparkSession.builder()
                .appName("Rule Engine Example")
                .master("local[*]")
                .config("spark.sql.adaptive.enabled", "true")
                .getOrCreate();

        try {
            // Example 1: Validation Rules
            example1ValidationRules(spark);

            // Example 2: Transformation Rules
            example2TransformationRules(spark);

            // Example 3: Business Rules
            example3BusinessRules(spark);

            // Example 4: Filter Rules
            example4FilterRules(spark);

            // Example 5: Chaining Multiple Rules
            example5ChainingRules(spark);

            logger.info("Rule Engine Example completed successfully!");

        } catch (Exception e) {
            logger.error("Rule Engine Example failed", e);
            System.exit(1);
        } finally {
            spark.stop();
        }
    }

    /**
     * Example 1: Validation Rules
     */
    private static void example1ValidationRules(SparkSession spark) {
        logger.info("\n" + "=".repeat(80));
        logger.info("EXAMPLE 1: Validation Rules");
        logger.info("=".repeat(80));

        // Create sample loan application data
        Dataset<Row> loanApplications = createLoanApplicationData(spark);

        logger.info("\nOriginal Data:");
        loanApplications.show(false);

        // Use pre-built loan eligibility validation rule
        ValidationRule eligibilityRule = BankingRuleTemplates.loanEligibilityRule();

        // Execute rule
        RuleExecutor executor = new RuleExecutor();
        Dataset<Row> validated = executor.execute(loanApplications, eligibilityRule);

        logger.info("\nAfter Loan Eligibility Validation:");
        validated.select("applicant_id", "age", "credit_score", "monthly_income",
                        "is_eligible")
                .show(false);

        // Count eligible vs ineligible
        long eligible = validated.filter("is_eligible = true").count();
        long ineligible = validated.filter("is_eligible = false").count();
        logger.info("Eligible: {}, Ineligible: {}", eligible, ineligible);
    }

    /**
     * Example 2: Transformation Rules
     */
    private static void example2TransformationRules(SparkSession spark) {
        logger.info("\n" + "=".repeat(80));
        logger.info("EXAMPLE 2: Transformation Rules");
        logger.info("=".repeat(80));

        // Create sample loan data
        Dataset<Row> loans = createLoanData(spark);

        logger.info("\nOriginal Loan Data:");
        loans.show(false);

        RuleExecutor executor = new RuleExecutor();

        // Apply credit score grading
        TransformationRule gradingRule = BankingRuleTemplates.creditScoreGradingRule();
        Dataset<Row> withGrade = executor.execute(loans, gradingRule);

        // Apply interest rate determination
        TransformationRule interestRule = BankingRuleTemplates.interestRateDeterminationRule();
        Dataset<Row> withRate = executor.execute(withGrade, interestRule);

        // Apply EMI calculation
        TransformationRule emiRule = BankingRuleTemplates.emiCalculationRule();
        Dataset<Row> withEmi = executor.execute(withRate, emiRule);

        logger.info("\nAfter Transformations:");
        withEmi.select("loan_id", "credit_score", "credit_grade",
                        "interest_rate", "loan_amount", "tenure_months", "monthly_emi")
                .show(false);
    }

    /**
     * Example 3: Business Rules
     */
    private static void example3BusinessRules(SparkSession spark) {
        logger.info("\n" + "=".repeat(80));
        logger.info("EXAMPLE 3: Business Rules");
        logger.info("=".repeat(80));

        // Create sample loan application data
        Dataset<Row> applications = createLoanApplicationData(spark);

        logger.info("\nOriginal Applications:");
        applications.select("applicant_id", "credit_score", "debt_to_income_ratio",
                        "employment_status")
                .show(false);

        // Apply loan approval business rule
        BusinessRule approvalRule = BankingRuleTemplates.loanApprovalRule();

        RuleExecutor executor = new RuleExecutor();
        Dataset<Row> withDecision = executor.execute(applications, approvalRule);

        logger.info("\nAfter Loan Approval Decision:");
        withDecision.select("applicant_id", "credit_score", "debt_to_income_ratio",
                        "loan_status", "approval_reason", "rejection_reason")
                .show(false);

        // Count approvals vs rejections
        long approved = withDecision.filter("loan_status = 'APPROVED'").count();
        long rejected = withDecision.filter("loan_status = 'REJECTED'").count();
        logger.info("Approved: {}, Rejected: {}", approved, rejected);
    }

    /**
     * Example 4: Filter Rules
     */
    private static void example4FilterRules(SparkSession spark) {
        logger.info("\n" + "=".repeat(80));
        logger.info("EXAMPLE 4: Filter Rules");
        logger.info("=".repeat(80));

        // Create sample transaction data
        Dataset<Row> transactions = createTransactionData(spark);

        logger.info("\nAll Transactions ({} records):", transactions.count());
        transactions.show(false);

        // Apply high-value transaction filter
        FilterRule highValueFilter = BankingRuleTemplates.highValueTransactionFilter();

        RuleExecutor executor = new RuleExecutor();
        Dataset<Row> highValueTxns = executor.execute(transactions, highValueFilter);

        logger.info("\nHigh-Value Transactions ({} records):", highValueTxns.count());
        highValueTxns.show(false);
    }

    /**
     * Example 5: Chaining Multiple Rules
     */
    private static void example5ChainingRules(SparkSession spark) {
        logger.info("\n" + "=".repeat(80));
        logger.info("EXAMPLE 5: Chaining Multiple Rules");
        logger.info("=".repeat(80));

        // Create comprehensive loan application data
        Dataset<Row> data = createComprehensiveLoanData(spark);

        logger.info("\nOriginal Data:");
        data.show(5, false);

        RuleExecutor executor = new RuleExecutor();
        Dataset<Row> result = data;

        // Rule 1: Validate eligibility
        logger.info("\n1. Validating eligibility...");
        result = executor.execute(result, BankingRuleTemplates.loanEligibilityRule());

        // Rule 2: Calculate credit grade
        logger.info("2. Calculating credit grade...");
        result = executor.execute(result, BankingRuleTemplates.creditScoreGradingRule());

        // Rule 3: Determine interest rate
        logger.info("3. Determining interest rate...");
        result = executor.execute(result, BankingRuleTemplates.interestRateDeterminationRule());

        // Rule 4: Make approval decision
        logger.info("4. Making approval decision...");
        result = executor.execute(result, BankingRuleTemplates.loanApprovalRule());

        // Rule 5: Calculate EMI for approved loans
        logger.info("5. Calculating EMI...");
        result = executor.execute(result, BankingRuleTemplates.emiCalculationRule());

        logger.info("\nFinal Results:");
        result.select("applicant_id", "is_eligible", "credit_grade",
                        "interest_rate", "loan_status", "monthly_emi")
                .show(false);

        // Summary statistics
        logger.info("\n" + "=".repeat(80));
        logger.info("SUMMARY STATISTICS");
        logger.info("=".repeat(80));
        logger.info("Total Applications: {}", result.count());
        logger.info("Eligible: {}", result.filter("is_eligible = true").count());
        logger.info("Approved: {}", result.filter("loan_status = 'APPROVED'").count());
        logger.info("Rejected: {}", result.filter("loan_status = 'REJECTED'").count());

        result.groupBy("credit_grade").count().show();
        result.groupBy("loan_status").count().show();
    }

    // Helper methods to create sample data

    private static Dataset<Row> createLoanApplicationData(SparkSession spark) {
        return spark.createDataFrame(
                List.of(
                        org.apache.spark.sql.RowFactory.create("A001", 28, 720, 5000, 0.35, "FULL_TIME", false),
                        org.apache.spark.sql.RowFactory.create("A002", 35, 580, 3500, 0.50, "SELF_EMPLOYED", false),
                        org.apache.spark.sql.RowFactory.create("A003", 42, 800, 8000, 0.25, "BUSINESS_OWNER", false),
                        org.apache.spark.sql.RowFactory.create("A004", 19, 650, 2500, 0.40, "PART_TIME", false),
                        org.apache.spark.sql.RowFactory.create("A005", 55, 700, 6000, 0.30, "FULL_TIME", true),
                        org.apache.spark.sql.RowFactory.create("A006", 31, 620, 4000, 0.45, "FULL_TIME", false)
                ),
                StructType.fromDDL("applicant_id STRING, age INT, credit_score INT, " +
                        "monthly_income DOUBLE, debt_to_income_ratio DOUBLE, " +
                        "employment_status STRING, bankruptcy_flag BOOLEAN")
        );
    }

    private static Dataset<Row> createLoanData(SparkSession spark) {
        return spark.createDataFrame(
                List.of(
                        org.apache.spark.sql.RowFactory.create("L001", 720, 100000, 0.06 / 12, 60),
                        org.apache.spark.sql.RowFactory.create("L002", 650, 50000, 0.07 / 12, 36),
                        org.apache.spark.sql.RowFactory.create("L003", 800, 200000, 0.05 / 12, 120),
                        org.apache.spark.sql.RowFactory.create("L004", 580, 30000, 0.10 / 12, 24)
                ),
                StructType.fromDDL("loan_id STRING, credit_score INT, loan_amount DOUBLE, " +
                        "monthly_interest_rate DOUBLE, tenure_months INT")
        );
    }

    private static Dataset<Row> createTransactionData(SparkSession spark) {
        return spark.createDataFrame(
                List.of(
                        org.apache.spark.sql.RowFactory.create("T001", 150.50, "PURCHASE"),
                        org.apache.spark.sql.RowFactory.create("T002", 15000.00, "WIRE"),
                        org.apache.spark.sql.RowFactory.create("T003", 5000.00, "WITHDRAWAL"),
                        org.apache.spark.sql.RowFactory.create("T004", 8500.00, "TRANSFER"),
                        org.apache.spark.sql.RowFactory.create("T005", 12000.00, "WIRE"),
                        org.apache.spark.sql.RowFactory.create("T006", 250.00, "PURCHASE")
                ),
                StructType.fromDDL("transaction_id STRING, amount DOUBLE, transaction_type STRING")
        );
    }

    private static Dataset<Row> createComprehensiveLoanData(SparkSession spark) {
        return spark.createDataFrame(
                List.of(
                        org.apache.spark.sql.RowFactory.create("A001", 28, 720, 5000, 0.35, "FULL_TIME", false, 100000, 60),
                        org.apache.spark.sql.RowFactory.create("A002", 35, 580, 3500, 0.50, "SELF_EMPLOYED", false, 50000, 36),
                        org.apache.spark.sql.RowFactory.create("A003", 42, 800, 8000, 0.25, "BUSINESS_OWNER", false, 200000, 120),
                        org.apache.spark.sql.RowFactory.create("A004", 19, 650, 2500, 0.40, "PART_TIME", false, 30000, 24),
                        org.apache.spark.sql.RowFactory.create("A005", 55, 700, 6000, 0.30, "FULL_TIME", true, 150000, 84)
                ),
                StructType.fromDDL("applicant_id STRING, age INT, credit_score INT, " +
                        "monthly_income DOUBLE, debt_to_income_ratio DOUBLE, " +
                        "employment_status STRING, bankruptcy_flag BOOLEAN, " +
                        "loan_amount DOUBLE, tenure_months INT")
        );
    }
}
