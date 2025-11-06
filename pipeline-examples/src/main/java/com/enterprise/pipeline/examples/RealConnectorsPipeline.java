package com.enterprise.pipeline.examples;

import com.enterprise.pipeline.rules.executor.RuleExecutor;
import com.enterprise.pipeline.rules.model.*;
import com.enterprise.pipeline.rules.template.BankingRuleTemplates;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static org.apache.spark.sql.functions.*;

/**
 * Real Connectors Pipeline - Complete Integration Example
 *
 * This pipeline demonstrates integration with REAL infrastructure:
 * - PostgreSQL (JDBC): Load loan applications, customers, transactions
 * - MinIO (S3): Load credit bureau data, save results
 * - Kafka: Publish results to topics
 *
 * Prerequisites:
 * 1. Start local environment: ./start-local-environment.sh
 * 2. Verify services are running: docker-compose ps
 * 3. Run this pipeline
 *
 * The pipeline performs:
 * 1. Load data from PostgreSQL (loans, customers, transactions)
 * 2. Enrich with MinIO/S3 data (credit bureau)
 * 3. Apply all rule types (validation, transformation, business, filter)
 * 4. Save results to PostgreSQL
 * 5. Publish to Kafka topics
 * 6. Upload to MinIO/S3
 *
 * @author Enterprise Pipeline Platform
 */
public class RealConnectorsPipeline {

    // Connection Configuration
    private static final String POSTGRES_URL = "jdbc:postgresql://localhost:5432/banking";
    private static final String POSTGRES_USER = "pipeline";
    private static final String POSTGRES_PASSWORD = "pipeline123";

    private static final String S3_ENDPOINT = "http://localhost:9000";
    private static final String S3_ACCESS_KEY = "minioadmin";
    private static final String S3_SECRET_KEY = "minioadmin";
    private static final String S3_BUCKET = "banking-data";

    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        // Create Spark session with S3 and Kafka support
        SparkSession spark = SparkSession.builder()
                .appName("Real Connectors Pipeline")
                .master("local[*]")
                .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
                .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
                .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.jars.packages",
                        "org.apache.hadoop:hadoop-aws:3.3.4," +
                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0," +
                        "org.postgresql:postgresql:42.6.0")
                .getOrCreate();

        System.out.println("=".repeat(80));
        System.out.println("🚀 Real Connectors Pipeline - Starting");
        System.out.println("=".repeat(80));

        try {
            // STEP 1: Load data from PostgreSQL
            System.out.println("\n📊 STEP 1: Loading data from PostgreSQL...");
            Dataset<Row> loanApplications = loadFromPostgres(spark, "loan_applications");
            Dataset<Row> customers = loadFromPostgres(spark, "customers");
            Dataset<Row> transactions = loadFromPostgres(spark, "transactions");

            System.out.println("  ✓ Loaded " + loanApplications.count() + " loan applications");
            System.out.println("  ✓ Loaded " + customers.count() + " customers");
            System.out.println("  ✓ Loaded " + transactions.count() + " transactions");

            // STEP 2: Load credit bureau data from MinIO (S3)
            System.out.println("\n📁 STEP 2: Loading credit bureau data from MinIO (S3)...");
            Dataset<Row> creditBureau = spark.read()
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv("s3a://" + S3_BUCKET + "/credit-bureau/credit_bureau.csv");

            System.out.println("  ✓ Loaded " + creditBureau.count() + " credit records from S3");

            // STEP 3: Enrich loan applications with customer and credit data
            System.out.println("\n🔗 STEP 3: Enriching loan applications...");
            Dataset<Row> enriched = loanApplications
                    .join(customers, "customer_id")
                    .join(creditBureau, "customer_id");

            // Calculate DTI
            enriched = enriched.withColumn("debt_to_income_ratio",
                    expr("monthly_debt_obligations / monthly_income"));

            System.out.println("  ✓ Enriched " + enriched.count() + " applications");

            // STEP 4: Apply Rule Engine
            System.out.println("\n🎯 STEP 4: Applying Rule Engine...");
            RuleExecutor executor = new RuleExecutor();

            // Validation Rule
            ValidationRule dataQualityRule = ValidationRule.builder()
                    .ruleId("dq_001")
                    .ruleName("loan_data_quality_check")
                    .conditions(ConditionGroup.and(
                            new Condition("loan_amount", Operator.GREATER_THAN, 0),
                            new Condition("credit_score", Operator.GREATER_THAN_OR_EQUAL, 300),
                            new Condition("monthly_income", Operator.GREATER_THAN, 0)
                    ))
                    .severity(Severity.ERROR)
                    .outputColumn("data_quality_passed")
                    .build();

            enriched = executor.execute(enriched, dataQualityRule);
            System.out.println("  ✓ Validation rule applied");

            // Credit Score Grading
            TransformationRule creditScoreRule = BankingRuleTemplates.creditScoreGradingRule();
            enriched = executor.execute(enriched, creditScoreRule);
            System.out.println("  ✓ Credit score grading applied");

            // Loan Approval Rule
            BusinessRule approvalRule = BankingRuleTemplates.loanApprovalRule();
            enriched = executor.execute(enriched, approvalRule);
            System.out.println("  ✓ Loan approval rule applied");

            // EMI Calculation
            TransformationRule emiRule = BankingRuleTemplates.emiCalculationRule();
            enriched = executor.execute(enriched, emiRule);
            System.out.println("  ✓ EMI calculation applied");

            // Fraud Detection
            BusinessRule fraudRule = BankingRuleTemplates.fraudDetectionRule();
            enriched = executor.execute(enriched, fraudRule);
            System.out.println("  ✓ Fraud detection applied");

            // STEP 5: Show results
            System.out.println("\n📊 STEP 5: Pipeline Results:");
            enriched.select(
                    "application_id", "name", "loan_amount", "credit_score",
                    "credit_score_grade", "loan_status", "monthly_emi", "fraud_flag"
            ).show(10, false);

            System.out.println("\n📈 Statistics:");
            enriched.groupBy("loan_status").count().show();
            enriched.groupBy("credit_score_grade").count().show();

            // STEP 6: Save results to PostgreSQL
            System.out.println("\n💾 STEP 6: Saving results to PostgreSQL...");
            Dataset<Row> results = enriched.select(
                    col("application_id"),
                    col("customer_id"),
                    col("loan_amount"),
                    col("loan_term_months"),
                    col("credit_score"),
                    col("credit_score_grade"),
                    col("debt_to_income_ratio"),
                    col("loan_status"),
                    col("approval_reason"),
                    col("rejection_reason"),
                    col("monthly_emi"),
                    lit(8.5).as("interest_rate"),
                    col("fraud_flag"),
                    col("fraud_reason"),
                    col("fraud_score"),
                    current_timestamp().as("processing_timestamp")
            );

            saveToPostgres(results, "loan_approval_results");
            System.out.println("  ✓ Results saved to PostgreSQL table: loan_approval_results");

            // STEP 7: Upload results to MinIO (S3)
            System.out.println("\n☁️ STEP 7: Uploading results to MinIO (S3)...");
            results.coalesce(1)
                    .write()
                    .mode(SaveMode.Overwrite)
                    .option("header", "true")
                    .csv("s3a://pipeline-output/loan-approval-results/");

            System.out.println("  ✓ Results uploaded to s3://pipeline-output/loan-approval-results/");

            // STEP 8: Publish to Kafka
            System.out.println("\n📡 STEP 8: Publishing to Kafka...");
            Dataset<Row> kafkaData = results.select(
                    col("application_id").as("key"),
                    to_json(struct("*")).as("value")
            );

            kafkaData.write()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                    .option("topic", "loan-approval-results")
                    .save();

            System.out.println("  ✓ Results published to Kafka topic: loan-approval-results");

            // STEP 9: Process Transactions (Fraud Detection)
            System.out.println("\n🔍 STEP 9: Processing transactions for fraud detection...");
            Dataset<Row> fraudResults = executor.execute(transactions, fraudRule);

            fraudResults.select("transaction_id", "customer_id", "amount", "fraud_flag", "fraud_reason")
                    .show(10, false);

            Dataset<Row> fraudSummary = fraudResults.select(
                    col("transaction_id"),
                    col("customer_id"),
                    col("amount"),
                    col("fraud_flag"),
                    col("fraud_score"),
                    col("fraud_reason"),
                    lit("HIGH").as("risk_level"),
                    current_timestamp().as("detected_at")
            );

            saveToPostgres(fraudSummary, "fraud_detection_results");
            System.out.println("  ✓ Fraud results saved to PostgreSQL");

            System.out.println("\n" + "=".repeat(80));
            System.out.println("✅ Pipeline Completed Successfully!");
            System.out.println("=".repeat(80));

            System.out.println("\n📊 Summary:");
            System.out.println("  ✓ Processed " + loanApplications.count() + " loan applications");
            System.out.println("  ✓ Analyzed " + transactions.count() + " transactions");
            System.out.println("  ✓ Applied 5 rule types");
            System.out.println("  ✓ Saved results to PostgreSQL");
            System.out.println("  ✓ Uploaded results to S3");
            System.out.println("  ✓ Published to Kafka");

            System.out.println("\n🔍 Verify Results:");
            System.out.println("  • PostgreSQL: SELECT * FROM loan_approval_results;");
            System.out.println("  • MinIO:      http://localhost:9001 → pipeline-output bucket");
            System.out.println("  • Kafka:      http://localhost:8080 → loan-approval-results topic");

        } catch (Exception e) {
            System.err.println("❌ Pipeline failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }

    /**
     * Load data from PostgreSQL
     */
    private static Dataset<Row> loadFromPostgres(SparkSession spark, String tableName) {
        Properties jdbcProps = new Properties();
        jdbcProps.put("user", POSTGRES_USER);
        jdbcProps.put("password", POSTGRES_PASSWORD);
        jdbcProps.put("driver", "org.postgresql.Driver");

        return spark.read()
                .jdbc(POSTGRES_URL, tableName, jdbcProps);
    }

    /**
     * Save data to PostgreSQL
     */
    private static void saveToPostgres(Dataset<Row> data, String tableName) {
        Properties jdbcProps = new Properties();
        jdbcProps.put("user", POSTGRES_USER);
        jdbcProps.put("password", POSTGRES_PASSWORD);
        jdbcProps.put("driver", "org.postgresql.Driver");

        data.write()
                .mode(SaveMode.Append)
                .jdbc(POSTGRES_URL, tableName, jdbcProps);
    }
}
