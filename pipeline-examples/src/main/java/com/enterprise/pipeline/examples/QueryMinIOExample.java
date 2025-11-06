package com.enterprise.pipeline.examples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import io.delta.tables.DeltaTable;

import static org.apache.spark.sql.functions.*;

/**
 * Query Data from MinIO (Local S3)
 *
 * This example demonstrates how to query data stored in MinIO using Spark.
 *
 * Queries Demonstrated:
 * 1. Read CSV files from MinIO
 * 2. Read Delta Lake tables from MinIO
 * 3. Aggregate queries
 * 4. Join multiple tables
 * 5. SQL queries
 * 6. Time travel (historical queries)
 * 7. View table history
 *
 * Prerequisites:
 * - MinIO running: ./start-local-environment.sh
 * - Data available: ./run-data-lake.sh
 *
 * Usage:
 *   mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.examples.QueryMinIOExample"
 *
 * @author Enterprise Pipeline Platform
 */
public class QueryMinIOExample {

    private static final String S3_ENDPOINT = "http://localhost:9000";
    private static final String S3_ACCESS_KEY = "minioadmin";
    private static final String S3_SECRET_KEY = "minioadmin";

    public static void main(String[] args) {
        // Create Spark session with MinIO and Delta Lake configuration
        SparkSession spark = SparkSession.builder()
            .appName("Query MinIO Data")
            .master("local[*]")

            // Delta Lake extensions
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")

            // MinIO (S3) configuration
            .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

            .getOrCreate();

        System.out.println("=".repeat(80));
        System.out.println("  🔍 Querying Data from MinIO (Local S3)");
        System.out.println("=".repeat(80));
        System.out.println();

        try {
            // ====================================================================
            // 1. Query CSV Files from MinIO
            // ====================================================================
            System.out.println("1️⃣  Reading CSV from MinIO...");
            System.out.println("-".repeat(80));

            Dataset<Row> customers = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("s3a://banking-data/customers/customers.csv");

            System.out.println("Total customers: " + customers.count());
            System.out.println("\nSample customer data:");
            customers.show(5, false);

            Dataset<Row> creditBureau = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("s3a://banking-data/credit-bureau/credit_bureau.csv");

            System.out.println("Total credit records: " + creditBureau.count());
            System.out.println();

            // ====================================================================
            // 2. Query Delta Lake Tables from MinIO
            // ====================================================================
            System.out.println("2️⃣  Reading Delta Lake from MinIO...");
            System.out.println("-".repeat(80));

            Dataset<Row> loansBronze = spark.read()
                .format("delta")
                .load("s3a://data-lake/bronze/loan_applications");

            System.out.println("Bronze Layer - Loan Applications:");
            System.out.println("  Total records: " + loansBronze.count());
            loansBronze.show(3, false);

            Dataset<Row> loansSilver = spark.read()
                .format("delta")
                .load("s3a://data-lake/silver/loan_applications_clean");

            System.out.println("\nSilver Layer - Cleaned Loan Applications:");
            System.out.println("  Total records: " + loansSilver.count());
            loansSilver.show(3, false);

            Dataset<Row> loansGold = spark.read()
                .format("delta")
                .load("s3a://data-lake/gold/loan_summary_by_purpose");

            System.out.println("\nGold Layer - Loan Summary:");
            loansGold.show(false);
            System.out.println();

            // ====================================================================
            // 3. Aggregate Queries
            // ====================================================================
            System.out.println("3️⃣  Aggregate Queries...");
            System.out.println("-".repeat(80));

            Dataset<Row> loanStats = loansSilver.groupBy("loan_purpose")
                .agg(
                    count("*").as("total_applications"),
                    sum("loan_amount").as("total_amount"),
                    avg("loan_amount").as("avg_amount"),
                    max("loan_amount").as("max_amount"),
                    min("loan_amount").as("min_amount")
                )
                .orderBy(desc("total_amount"));

            System.out.println("Loan Statistics by Purpose:");
            loanStats.show(false);
            System.out.println();

            // ====================================================================
            // 4. Join Multiple Tables
            // ====================================================================
            System.out.println("4️⃣  Join Queries...");
            System.out.println("-".repeat(80));

            Dataset<Row> customersClean = spark.read()
                .format("delta")
                .load("s3a://data-lake/silver/customers_clean");

            Dataset<Row> enrichedLoans = loansSilver.join(
                customersClean,
                loansSilver.col("customer_id").equalTo(customersClean.col("customer_id")),
                "inner"
            ).select(
                loansSilver.col("application_id"),
                loansSilver.col("applicant_name"),
                loansSilver.col("loan_amount"),
                loansSilver.col("loan_purpose"),
                customersClean.col("monthly_income"),
                customersClean.col("employment_status")
            );

            System.out.println("Enriched Loan Data (with Customer Info):");
            enrichedLoans.show(false);

            // Calculate loan-to-income ratio
            Dataset<Row> loanToIncome = enrichedLoans
                .withColumn("loan_to_income_ratio",
                    round(col("loan_amount").divide(col("monthly_income")), 2))
                .select("applicant_name", "loan_amount", "monthly_income",
                        "loan_to_income_ratio", "employment_status")
                .orderBy(desc("loan_to_income_ratio"));

            System.out.println("\nLoan-to-Income Analysis:");
            loanToIncome.show(false);
            System.out.println();

            // ====================================================================
            // 5. SQL Queries
            // ====================================================================
            System.out.println("5️⃣  SQL Queries...");
            System.out.println("-".repeat(80));

            loansSilver.createOrReplaceTempView("loans");
            customersClean.createOrReplaceTempView("customers");

            Dataset<Row> highValueLoans = spark.sql(
                "SELECT " +
                "  application_id, " +
                "  applicant_name, " +
                "  loan_amount, " +
                "  loan_purpose, " +
                "  applicant_age " +
                "FROM loans " +
                "WHERE loan_amount > 500000 " +
                "ORDER BY loan_amount DESC"
            );

            System.out.println("High-Value Loans (> 500,000):");
            highValueLoans.show(false);

            Dataset<Row> employmentStats = spark.sql(
                "SELECT " +
                "  employment_status, " +
                "  COUNT(*) as customer_count, " +
                "  AVG(monthly_income) as avg_income, " +
                "  MAX(monthly_income) as max_income " +
                "FROM customers " +
                "GROUP BY employment_status " +
                "ORDER BY avg_income DESC"
            );

            System.out.println("\nEmployment Statistics:");
            employmentStats.show(false);
            System.out.println();

            // ====================================================================
            // 6. Time Travel - Query Historical Versions
            // ====================================================================
            System.out.println("6️⃣  Time Travel - Historical Queries...");
            System.out.println("-".repeat(80));

            try {
                Dataset<Row> version0 = spark.read()
                    .format("delta")
                    .option("versionAsOf", "0")
                    .load("s3a://data-lake/silver/loan_applications_clean");

                System.out.println("Version 0 (Initial Load):");
                System.out.println("  Record count: " + version0.count());
                version0.show(3, false);

                // Show current version
                System.out.println("\nCurrent Version:");
                System.out.println("  Record count: " + loansSilver.count());
                loansSilver.show(3, false);
            } catch (Exception e) {
                System.out.println("Note: Time travel requires multiple versions");
                System.out.println("Run the Data Lake pipeline multiple times to see version history");
            }
            System.out.println();

            // ====================================================================
            // 7. Delta Table History
            // ====================================================================
            System.out.println("7️⃣  Delta Table History...");
            System.out.println("-".repeat(80));

            DeltaTable deltaTable = DeltaTable.forPath(spark,
                "s3a://data-lake/silver/loan_applications_clean");

            System.out.println("Table History (Recent Operations):");
            deltaTable.history(10)
                .select("version", "timestamp", "operation", "operationMetrics")
                .show(10, false);

            System.out.println("\nTable Details:");
            deltaTable.detail().show(false);
            System.out.println();

            // ====================================================================
            // 8. Filter and Export
            // ====================================================================
            System.out.println("8️⃣  Filter and Export Example...");
            System.out.println("-".repeat(80));

            Dataset<Row> homeLoans = loansSilver
                .filter("loan_purpose = 'HOME_LOAN'")
                .select("application_id", "applicant_name", "loan_amount",
                        "loan_term_months", "applicant_age");

            System.out.println("Home Loans Only:");
            homeLoans.show(false);

            // Example: Export to MinIO as CSV
            System.out.println("\nExporting home loans to MinIO as CSV...");
            homeLoans.coalesce(1)
                .write()
                .option("header", "true")
                .mode("overwrite")
                .csv("s3a://reports/home-loans-export");

            System.out.println("✓ Exported to: s3a://reports/home-loans-export/");
            System.out.println("  View in MinIO Console: http://localhost:9001");
            System.out.println();

            // ====================================================================
            // Summary
            // ====================================================================
            System.out.println("=".repeat(80));
            System.out.println("  ✅ All Queries Completed Successfully!");
            System.out.println("=".repeat(80));
            System.out.println();

            System.out.println("📊 Data Sources Queried:");
            System.out.println("  • CSV Files:        s3a://banking-data/");
            System.out.println("  • Bronze Layer:     s3a://data-lake/bronze/");
            System.out.println("  • Silver Layer:     s3a://data-lake/silver/");
            System.out.println("  • Gold Layer:       s3a://data-lake/gold/");
            System.out.println();

            System.out.println("🔍 Query Types Demonstrated:");
            System.out.println("  ✓ CSV file reads");
            System.out.println("  ✓ Delta Lake reads");
            System.out.println("  ✓ Aggregations");
            System.out.println("  ✓ Joins");
            System.out.println("  ✓ SQL queries");
            System.out.println("  ✓ Time travel");
            System.out.println("  ✓ Table history");
            System.out.println("  ✓ Export to CSV");
            System.out.println();

            System.out.println("🌐 Access Your Data:");
            System.out.println("  MinIO Console:  http://localhost:9001");
            System.out.println("  Username:       minioadmin");
            System.out.println("  Password:       minioadmin");
            System.out.println();

            System.out.println("📚 Documentation:");
            System.out.println("  QUERY_MINIO_DATA.md  - Complete querying guide");
            System.out.println("  DATA_LAKE_GUIDE.md   - Data Lake architecture guide");

        } catch (Exception e) {
            System.err.println("=".repeat(80));
            System.err.println("  ❌ Query Failed!");
            System.err.println("=".repeat(80));
            System.err.println();
            System.err.println("Error: " + e.getMessage());
            System.err.println();
            System.err.println("Troubleshooting:");
            System.err.println("  1. Ensure MinIO is running:");
            System.err.println("     docker ps | grep minio");
            System.err.println();
            System.err.println("  2. Ensure Data Lake is created:");
            System.err.println("     ./run-data-lake.sh");
            System.err.println();
            System.err.println("  3. Check MinIO console:");
            System.err.println("     http://localhost:9001");
            System.err.println();
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }
}
