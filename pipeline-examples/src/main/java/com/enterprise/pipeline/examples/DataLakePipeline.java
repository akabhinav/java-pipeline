package com.enterprise.pipeline.examples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import io.delta.tables.DeltaTable;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.apache.spark.sql.functions.*;

/**
 * Data Lake Pipeline - Medallion Architecture (Bronze/Silver/Gold)
 *
 * This demonstrates building a complete Data Lake on MinIO using Delta Lake format.
 *
 * Architecture:
 * - Bronze Layer: Raw data (append-only, immutable)
 * - Silver Layer: Cleaned, validated data
 * - Gold Layer: Business-ready aggregates
 *
 * Features:
 * - ACID transactions
 * - Time travel
 * - Schema evolution
 * - Upserts (merge)
 * - Data versioning
 *
 * Storage: MinIO (S3-compatible) at s3a://data-lake/
 *
 * @author Enterprise Pipeline Platform
 */
public class DataLakePipeline {

    private static final String S3_ENDPOINT = "http://localhost:9000";
    private static final String S3_ACCESS_KEY = "minioadmin";
    private static final String S3_SECRET_KEY = "minioadmin";

    // Data Lake paths on MinIO
    private static final String BRONZE_PATH = "s3a://data-lake/bronze";
    private static final String SILVER_PATH = "s3a://data-lake/silver";
    private static final String GOLD_PATH = "s3a://data-lake/gold";

    public static void main(String[] args) {
        // Create Spark session with Delta Lake support
        SparkSession spark = SparkSession.builder()
                .appName("Data Lake Pipeline")
                .master("local[*]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
                .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
                .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .getOrCreate();

        System.out.println("=".repeat(80));
        System.out.println("🏗️  Data Lake Pipeline - Medallion Architecture");
        System.out.println("=".repeat(80));

        try {
            // Step 1: Bronze Layer - Load raw data
            System.out.println("\n📥 BRONZE LAYER: Loading raw data...");
            Dataset<Row> rawLoanData = loadRawLoanData(spark);
            writeToBronze(rawLoanData, "loan_applications");
            System.out.println("✓ Bronze: " + rawLoanData.count() + " records written");

            Dataset<Row> rawCustomerData = loadRawCustomerData(spark);
            writeToBronze(rawCustomerData, "customers");
            System.out.println("✓ Bronze: " + rawCustomerData.count() + " customer records written");

            // Step 2: Silver Layer - Clean and validate
            System.out.println("\n🔄 SILVER LAYER: Cleaning and validating...");
            Dataset<Row> cleanedLoans = cleanLoans(spark, BRONZE_PATH + "/loan_applications");
            writeToSilver(cleanedLoans, "loan_applications_clean");
            System.out.println("✓ Silver: " + cleanedLoans.count() + " records cleaned");

            Dataset<Row> cleanedCustomers = cleanCustomers(spark, BRONZE_PATH + "/customers");
            writeToSilver(cleanedCustomers, "customers_clean");
            System.out.println("✓ Silver: " + cleanedCustomers.count() + " customers cleaned");

            // Step 3: Gold Layer - Business aggregates
            System.out.println("\n🏆 GOLD LAYER: Creating business aggregates...");
            Dataset<Row> loanSummary = createLoanSummary(spark);
            writeToGold(loanSummary, "loan_summary_by_purpose");
            System.out.println("✓ Gold: Loan summary created");

            Dataset<Row> customerMetrics = createCustomerMetrics(spark);
            writeToGold(customerMetrics, "customer_metrics");
            System.out.println("✓ Gold: Customer metrics created");

            // Step 4: Delta Lake Features
            System.out.println("\n⚡ DELTA LAKE FEATURES:");

            // Time Travel
            System.out.println("\n📜 Time Travel - View historical versions:");
            showTimeTravel(spark, SILVER_PATH + "/loan_applications_clean");

            // Merge/Upsert
            System.out.println("\n🔀 Upsert - Merge new data:");
            performUpsert(spark);

            // Schema Evolution
            System.out.println("\n📐 Schema Evolution:");
            demonstrateSchemaEvolution(spark);

            // Statistics
            System.out.println("\n📊 DATA LAKE STATISTICS:");
            showDataLakeStats(spark);

            System.out.println("\n" + "=".repeat(80));
            System.out.println("✅ Data Lake Pipeline Completed Successfully!");
            System.out.println("=".repeat(80));

            System.out.println("\n🔍 Access your Data Lake:");
            System.out.println("  MinIO Console: http://localhost:9001");
            System.out.println("  Bucket: data-lake");
            System.out.println("  Layers: bronze/, silver/, gold/");

            System.out.println("\n📚 Query your Data Lake:");
            System.out.println("  spark.read.format(\"delta\").load(\"s3a://data-lake/gold/loan_summary_by_purpose\")");

        } catch (Exception e) {
            System.err.println("❌ Pipeline failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }

    // ========================================================================
    // BRONZE LAYER - Raw Data Ingestion
    // ========================================================================

    private static void writeToBronze(Dataset<Row> data, String tableName) {
        data.withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_system", lit("banking_system"))
            .write()
            .format("delta")
            .mode(SaveMode.Append)
            .option("mergeSchema", "true")
            .save(BRONZE_PATH + "/" + tableName);
    }

    // ========================================================================
    // SILVER LAYER - Cleaned & Validated Data
    // ========================================================================

    private static Dataset<Row> cleanLoans(SparkSession spark, String bronzePath) {
        Dataset<Row> bronze = spark.read().format("delta").load(bronzePath);

        return bronze
            // Remove duplicates
            .dropDuplicates("application_id")
            // Validate data quality
            .filter("loan_amount > 0")
            .filter("applicant_age >= 18 AND applicant_age <= 100")
            // Add quality flags
            .withColumn("data_quality_score", lit(1.0))
            .withColumn("cleaned_timestamp", current_timestamp());
    }

    private static Dataset<Row> cleanCustomers(SparkSession spark, String bronzePath) {
        Dataset<Row> bronze = spark.read().format("delta").load(bronzePath);

        return bronze
            .dropDuplicates("customer_id")
            .filter("email IS NOT NULL")
            .filter("monthly_income > 0")
            .withColumn("data_quality_score", lit(1.0))
            .withColumn("cleaned_timestamp", current_timestamp());
    }

    private static void writeToSilver(Dataset<Row> data, String tableName) {
        data.write()
            .format("delta")
            .mode(SaveMode.Overwrite)
            .option("overwriteSchema", "true")
            .save(SILVER_PATH + "/" + tableName);
    }

    // ========================================================================
    // GOLD LAYER - Business-Ready Aggregates
    // ========================================================================

    private static Dataset<Row> createLoanSummary(SparkSession spark) {
        Dataset<Row> silver = spark.read()
            .format("delta")
            .load(SILVER_PATH + "/loan_applications_clean");

        return silver.groupBy("loan_purpose")
            .agg(
                count("*").as("total_applications"),
                sum("loan_amount").as("total_amount"),
                avg("loan_amount").as("avg_amount"),
                avg("applicant_age").as("avg_applicant_age")
            )
            .withColumn("updated_at", current_timestamp());
    }

    private static Dataset<Row> createCustomerMetrics(SparkSession spark) {
        Dataset<Row> customers = spark.read()
            .format("delta")
            .load(SILVER_PATH + "/customers_clean");

        return customers.groupBy("employment_status")
            .agg(
                count("*").as("customer_count"),
                avg("monthly_income").as("avg_income"),
                sum("monthly_income").as("total_income")
            )
            .withColumn("updated_at", current_timestamp());
    }

    private static void writeToGold(Dataset<Row> data, String tableName) {
        data.write()
            .format("delta")
            .mode(SaveMode.Overwrite)
            .save(GOLD_PATH + "/" + tableName);
    }

    // ========================================================================
    // DELTA LAKE FEATURES
    // ========================================================================

    private static void showTimeTravel(SparkSession spark, String deltaPath) {
        // Show table history
        DeltaTable deltaTable = DeltaTable.forPath(spark, deltaPath);
        System.out.println("📋 Table History:");
        deltaTable.history(5).select("version", "timestamp", "operation", "operationMetrics")
            .show(5, false);

        // Query version 0 (initial load)
        System.out.println("🕐 Data at version 0:");
        Dataset<Row> version0 = spark.read()
            .format("delta")
            .option("versionAsOf", "0")
            .load(deltaPath);
        System.out.println("  Records: " + version0.count());
    }

    private static void performUpsert(SparkSession spark) {
        String silverPath = SILVER_PATH + "/loan_applications_clean";
        DeltaTable silverTable = DeltaTable.forPath(spark, silverPath);

        // Create new/updated records
        Dataset<Row> updates = loadRawLoanData(spark)
            .limit(5)
            .withColumn("loan_amount", col("loan_amount").multiply(1.1))
            .withColumn("data_quality_score", lit(1.0))
            .withColumn("cleaned_timestamp", current_timestamp());

        // Merge (upsert)
        silverTable.as("target")
            .merge(
                updates.as("source"),
                "target.application_id = source.application_id"
            )
            .whenMatched()
            .updateAll()
            .whenNotMatched()
            .insertAll()
            .execute();

        System.out.println("✓ Upsert completed - 5 records merged");
    }

    private static void demonstrateSchemaEvolution(SparkSession spark) {
        // Add new column to existing Delta table
        Dataset<Row> withNewColumn = spark.read()
            .format("delta")
            .load(SILVER_PATH + "/loan_applications_clean")
            .withColumn("risk_category", lit("MEDIUM"));

        withNewColumn.write()
            .format("delta")
            .mode(SaveMode.Overwrite)
            .option("mergeSchema", "true")
            .save(SILVER_PATH + "/loan_applications_clean");

        System.out.println("✓ Schema evolved - added 'risk_category' column");
    }

    private static void showDataLakeStats(SparkSession spark) {
        System.out.println("\n🏗️  BRONZE LAYER:");
        showLayerStats(spark, BRONZE_PATH);

        System.out.println("\n🔄 SILVER LAYER:");
        showLayerStats(spark, SILVER_PATH);

        System.out.println("\n🏆 GOLD LAYER:");
        showLayerStats(spark, GOLD_PATH);
    }

    private static void showLayerStats(SparkSession spark, String layerPath) {
        try {
            // This would list all tables in the layer
            System.out.println("  Path: " + layerPath);
            System.out.println("  Format: Delta Lake");
            System.out.println("  Features: ACID, Time Travel, Schema Evolution");
        } catch (Exception e) {
            System.out.println("  No tables yet");
        }
    }

    // ========================================================================
    // SAMPLE DATA GENERATION
    // ========================================================================

    private static Dataset<Row> loadRawLoanData(SparkSession spark) {
        return spark.createDataFrame(
            java.util.Arrays.asList(
                org.apache.spark.sql.RowFactory.create("LA001", "C001", "John Doe", 35, 500000.0, 240, "HOME_LOAN"),
                org.apache.spark.sql.RowFactory.create("LA002", "C002", "Jane Smith", 28, 150000.0, 60, "PERSONAL_LOAN"),
                org.apache.spark.sql.RowFactory.create("LA003", "C003", "Bob Johnson", 45, 2000000.0, 180, "BUSINESS_LOAN"),
                org.apache.spark.sql.RowFactory.create("LA004", "C004", "Alice Williams", 32, 800000.0, 84, "CAR_LOAN"),
                org.apache.spark.sql.RowFactory.create("LA005", "C005", "Charlie Brown", 55, 300000.0, 36, "PERSONAL_LOAN")
            ),
            new org.apache.spark.sql.types.StructType()
                .add("application_id", org.apache.spark.sql.types.DataTypes.StringType)
                .add("customer_id", org.apache.spark.sql.types.DataTypes.StringType)
                .add("applicant_name", org.apache.spark.sql.types.DataTypes.StringType)
                .add("applicant_age", org.apache.spark.sql.types.DataTypes.IntegerType)
                .add("loan_amount", org.apache.spark.sql.types.DataTypes.DoubleType)
                .add("loan_term_months", org.apache.spark.sql.types.DataTypes.IntegerType)
                .add("loan_purpose", org.apache.spark.sql.types.DataTypes.StringType)
        );
    }

    private static Dataset<Row> loadRawCustomerData(SparkSession spark) {
        return spark.createDataFrame(
            java.util.Arrays.asList(
                org.apache.spark.sql.RowFactory.create("C001", "John Doe", "john@example.com", 120000.0, "FULL_TIME"),
                org.apache.spark.sql.RowFactory.create("C002", "Jane Smith", "jane@example.com", 85000.0, "FULL_TIME"),
                org.apache.spark.sql.RowFactory.create("C003", "Bob Johnson", "bob@example.com", 200000.0, "BUSINESS_OWNER"),
                org.apache.spark.sql.RowFactory.create("C004", "Alice Williams", "alice@example.com", 95000.0, "FULL_TIME"),
                org.apache.spark.sql.RowFactory.create("C005", "Charlie Brown", "charlie@example.com", 110000.0, "SELF_EMPLOYED")
            ),
            new org.apache.spark.sql.types.StructType()
                .add("customer_id", org.apache.spark.sql.types.DataTypes.StringType)
                .add("name", org.apache.spark.sql.types.DataTypes.StringType)
                .add("email", org.apache.spark.sql.types.DataTypes.StringType)
                .add("monthly_income", org.apache.spark.sql.types.DataTypes.DoubleType)
                .add("employment_status", org.apache.spark.sql.types.DataTypes.StringType)
        );
    }
}
