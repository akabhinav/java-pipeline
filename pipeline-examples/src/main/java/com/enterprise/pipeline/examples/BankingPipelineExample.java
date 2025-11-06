package com.enterprise.pipeline.examples;

import com.enterprise.pipeline.api.PipelineException;
import com.enterprise.pipeline.api.model.PipelineConfig;
import com.enterprise.pipeline.core.builder.PipelineBuilder;
import com.enterprise.pipeline.core.engine.PipelineEngine;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Comprehensive banking pipeline example.
 *
 * This example demonstrates:
 * 1. Reading from multiple sources (CSV files)
 * 2. Complex transformations (filtering, joins, aggregations)
 * 3. Data quality checks (null handling, validation)
 * 4. Multiple join operations
 * 5. Aggregations and analytics
 * 6. Writing to Parquet with partitioning
 *
 * Business Scenario:
 * - Process daily banking transactions
 * - Join with customer and account data
 * - Filter fraudulent transactions
 * - Calculate customer risk scores
 * - Generate daily summaries by region
 *
 * @author Enterprise Data Pipeline Team
 */
public class BankingPipelineExample {

    private static final Logger logger = LoggerFactory.getLogger(BankingPipelineExample.class);

    public static void main(String[] args) {
        logger.info("Starting Banking Pipeline Example");

        SparkSession spark = SparkSession.builder()
                .appName("Banking Pipeline Example")
                .master("local[*]")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .getOrCreate();

        try {
            // Create sample data
            createSampleBankingData(spark);

            // Build comprehensive banking pipeline
            executeBankingPipeline(spark);

            // Show results
            logger.info("=== Transaction Summary by Region ===");
            spark.read().parquet("/tmp/output/banking/transaction_summary")
                    .orderBy("region", "risk_category")
                    .show(20, false);

            logger.info("=== High-Risk Transactions ===");
            spark.read().parquet("/tmp/output/banking/high_risk_transactions")
                    .show(10, false);

        } catch (Exception e) {
            logger.error("Banking pipeline failed", e);
            System.exit(1);
        } finally {
            spark.stop();
        }
    }

    /**
     * Execute the main banking pipeline.
     */
    private static void executeBankingPipeline(SparkSession spark) throws PipelineException {
        logger.info("Building banking pipeline...");

        // Pipeline 1: Process customer data
        PipelineConfig customerPipeline = PipelineBuilder.create("customer-processing")
                .fromSource("file", "customers", Map.of(
                        "path", "/tmp/banking/customers.csv",
                        "format", "csv",
                        "options", Map.of("header", "true", "inferSchema", "true")
                ))
                .transform("dropNa", Map.of(
                        "how", "any",
                        "columns", List.of("customer_id", "name", "region")
                ))
                .transform("filter", Map.of(
                        "condition", "status = 'active'"
                ))
                .toSink("file", Map.of(
                        "path", "/tmp/output/banking/customers_processed.parquet",
                        "format", "parquet",
                        "mode", "overwrite"
                ))
                .build();

        PipelineEngine engine = new PipelineEngine(spark);
        engine.execute(customerPipeline);

        // Pipeline 2: Process transactions with joins and aggregations
        PipelineConfig transactionPipeline = PipelineBuilder.create("transaction-processing")
                // Read transactions
                .fromSource("file", "transactions", Map.of(
                        "path", "/tmp/banking/transactions.csv",
                        "format", "csv",
                        "options", Map.of("header", "true", "inferSchema", "true")
                ))
                // Clean data
                .transform("dropNa", Map.of(
                        "how", "any",
                        "columns", List.of("transaction_id", "customer_id", "amount")
                ))
                .transform("filter", Map.of(
                        "condition", "amount > 0"
                ))
                // Add risk scoring
                .transform("withColumn", Map.of(
                        "columnName", "risk_score",
                        "expression", "CASE " +
                                "WHEN amount > 10000 THEN 'HIGH' " +
                                "WHEN amount > 5000 THEN 'MEDIUM' " +
                                "ELSE 'LOW' END"
                ))
                .transform("withColumn", Map.of(
                        "columnName", "transaction_date",
                        "expression", "to_date(timestamp)"
                ))
                // Register for joins
                .toSink("file", Map.of(
                        "path", "/tmp/output/banking/transactions_intermediate.parquet",
                        "format", "parquet",
                        "mode", "overwrite"
                ))
                .build();

        engine.execute(transactionPipeline);

        // Pipeline 3: Join transactions with customers and create summary
        PipelineConfig analyticsPipeline = PipelineBuilder.create("transaction-analytics")
                .fromSource("file", "transactions", Map.of(
                        "path", "/tmp/output/banking/transactions_intermediate.parquet",
                        "format", "parquet"
                ))
                .fromSource("file", "customers", Map.of(
                        "path", "/tmp/output/banking/customers_processed.parquet",
                        "format", "parquet"
                ))
                .transform("innerJoin", Map.of(
                        "rightDataset", "customers",
                        "joinColumns", List.of("customer_id")
                ))
                .transform("select", Map.of(
                        "columns", List.of("transaction_id", "customer_id", "name",
                                "region", "amount", "risk_score", "transaction_date")
                ))
                .transform("groupBy", "summary", Map.of(
                        "columns", List.of("region", "risk_score"),
                        "aggregations", Map.of(
                                "amount", "sum",
                                "transaction_id", "count"
                        )
                ))
                .transform("withColumn", Map.of(
                        "columnName", "risk_category",
                        "expression", "risk_score"
                ))
                .toSink("file", Map.of(
                        "path", "/tmp/output/banking/transaction_summary.parquet",
                        "format", "parquet",
                        "mode", "overwrite"
                ))
                .build();

        engine.execute(analyticsPipeline);

        // Pipeline 4: Extract high-risk transactions
        PipelineConfig highRiskPipeline = PipelineBuilder.create("high-risk-transactions")
                .fromSource("file", Map.of(
                        "path", "/tmp/output/banking/transactions_intermediate.parquet",
                        "format", "parquet"
                ))
                .transform("filter", Map.of(
                        "condition", "risk_score = 'HIGH'"
                ))
                .transform("orderBy", Map.of(
                        "columns", List.of("amount"),
                        "ascending", false
                ))
                .toSink("file", Map.of(
                        "path", "/tmp/output/banking/high_risk_transactions.parquet",
                        "format", "parquet",
                        "mode", "overwrite"
                ))
                .build();

        engine.execute(highRiskPipeline);
    }

    /**
     * Create sample banking data for demonstration.
     */
    private static void createSampleBankingData(SparkSession spark) {
        logger.info("Creating sample banking data...");

        // Sample customers
        String customersCSV = """
                customer_id,name,region,status,credit_score
                C001,John Smith,North,active,750
                C002,Jane Doe,South,active,680
                C003,Bob Johnson,East,active,720
                C004,Alice Williams,West,active,800
                C005,Charlie Brown,North,inactive,650
                C006,Diana Prince,South,active,790
                C007,Eve Davis,East,active,710
                C008,Frank Miller,West,active,740
                C009,Grace Lee,North,active,820
                C010,Henry Wilson,South,active,760
                """;

        // Sample transactions
        String transactionsCSV = """
                transaction_id,customer_id,amount,timestamp,type
                T001,C001,150.50,2024-01-15 10:30:00,purchase
                T002,C002,12000.00,2024-01-15 11:00:00,transfer
                T003,C003,5500.00,2024-01-15 12:00:00,withdrawal
                T004,C001,250.00,2024-01-15 13:00:00,purchase
                T005,C004,15000.00,2024-01-15 14:00:00,transfer
                T006,C002,3200.00,2024-01-15 15:00:00,purchase
                T007,C006,8500.00,2024-01-16 09:00:00,transfer
                T008,C007,450.00,2024-01-16 10:00:00,purchase
                T009,C008,11000.00,2024-01-16 11:00:00,withdrawal
                T010,C009,6200.00,2024-01-16 12:00:00,transfer
                T011,C001,18000.00,2024-01-16 13:00:00,transfer
                T012,C010,4100.00,2024-01-16 14:00:00,purchase
                T013,C003,13500.00,2024-01-17 09:00:00,transfer
                T014,C004,2200.00,2024-01-17 10:00:00,purchase
                T015,C006,9800.00,2024-01-17 11:00:00,withdrawal
                """;

        try {
            // Write sample data
            java.nio.file.Files.createDirectories(java.nio.file.Path.of("/tmp/banking"));
            java.nio.file.Files.writeString(
                    java.nio.file.Path.of("/tmp/banking/customers.csv"),
                    customersCSV
            );
            java.nio.file.Files.writeString(
                    java.nio.file.Path.of("/tmp/banking/transactions.csv"),
                    transactionsCSV
            );

            logger.info("Sample banking data created");
        } catch (Exception e) {
            logger.error("Failed to create sample data", e);
            throw new RuntimeException(e);
        }
    }
}
