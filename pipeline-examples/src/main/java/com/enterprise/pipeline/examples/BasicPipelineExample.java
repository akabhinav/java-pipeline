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
 * Basic pipeline example using the Fluent Builder API.
 *
 * This example demonstrates:
 * 1. Creating a pipeline programmatically
 * 2. Reading CSV data
 * 3. Applying transformations (select, filter, withColumn)
 * 4. Writing to Parquet
 *
 * @author Enterprise Data Pipeline Team
 */
public class BasicPipelineExample {

    private static final Logger logger = LoggerFactory.getLogger(BasicPipelineExample.class);

    public static void main(String[] args) {
        logger.info("Starting Basic Pipeline Example");

        // Create Spark session
        SparkSession spark = SparkSession.builder()
                .appName("Basic Pipeline Example")
                .master("local[*]")
                .config("spark.sql.adaptive.enabled", "true")
                .getOrCreate();

        try {
            // Create sample input data
            createSampleData(spark);

            // Build pipeline using Fluent API
            PipelineConfig config = PipelineBuilder.create("basic-customer-pipeline")
                    .version("1.0")
                    .fromSource("file", Map.of(
                            "path", "/tmp/customers.csv",
                            "format", "csv",
                            "options", Map.of(
                                    "header", "true",
                                    "inferSchema", "true"
                            )
                    ))
                    .transform("select", Map.of(
                            "columns", List.of("customer_id", "name", "age", "city", "purchase_amount")
                    ))
                    .transform("filter", Map.of(
                            "condition", "age >= 18 AND purchase_amount > 100"
                    ))
                    .transform("withColumn", Map.of(
                            "columnName", "customer_segment",
                            "expression", "CASE WHEN purchase_amount > 1000 THEN 'Premium' " +
                                    "WHEN purchase_amount > 500 THEN 'Gold' " +
                                    "ELSE 'Standard' END"
                    ))
                    .toSink("file", Map.of(
                            "path", "/tmp/output/customers_processed.parquet",
                            "format", "parquet",
                            "mode", "overwrite"
                    ))
                    .build();

            logger.info("Pipeline configuration created: {}", config.name());

            // Execute pipeline
            PipelineEngine engine = new PipelineEngine(spark);
            engine.execute(config);

            logger.info("Pipeline execution completed successfully");

            // Show results
            spark.read().parquet("/tmp/output/customers_processed.parquet").show();

        } catch (PipelineException e) {
            logger.error("Pipeline execution failed", e);
            System.exit(1);
        } finally {
            spark.stop();
        }
    }

    /**
     * Create sample CSV data for demonstration.
     */
    private static void createSampleData(SparkSession spark) {
        logger.info("Creating sample data...");

        // Create sample data
        String csvContent = """
                customer_id,name,age,city,purchase_amount
                1,John Doe,25,New York,150.50
                2,Jane Smith,17,Los Angeles,50.00
                3,Bob Johnson,35,Chicago,1200.00
                4,Alice Williams,42,Houston,800.00
                5,Charlie Brown,19,Phoenix,250.00
                6,Diana Prince,28,Philadelphia,1500.00
                7,Eve Davis,16,San Antonio,30.00
                8,Frank Miller,55,San Diego,600.00
                9,Grace Lee,33,Dallas,900.00
                10,Henry Wilson,45,San Jose,2000.00
                """;

        // Write sample data to temp file
        try {
            java.nio.file.Files.writeString(
                    java.nio.file.Path.of("/tmp/customers.csv"),
                    csvContent
            );
            logger.info("Sample data created at /tmp/customers.csv");
        } catch (Exception e) {
            logger.error("Failed to create sample data", e);
            throw new RuntimeException(e);
        }
    }
}
