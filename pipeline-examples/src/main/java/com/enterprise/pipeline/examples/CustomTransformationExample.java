package com.enterprise.pipeline.examples;

import com.enterprise.pipeline.api.PipelineException;
import com.enterprise.pipeline.api.model.PipelineConfig;
import com.enterprise.pipeline.core.builder.PipelineBuilder;
import com.enterprise.pipeline.core.engine.PipelineEngine;
import com.enterprise.pipeline.core.registry.TransformationRegistry;
import com.enterprise.pipeline.examples.custom.MaskSensitiveDataTransform;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Example demonstrating how to register and use custom transformations.
 *
 * This shows:
 * 1. Creating a custom transformation class
 * 2. Registering it with the TransformationRegistry
 * 3. Using it in a pipeline
 *
 * @author Enterprise Data Pipeline Team
 */
public class CustomTransformationExample {

    private static final Logger logger = LoggerFactory.getLogger(CustomTransformationExample.class);

    public static void main(String[] args) {
        logger.info("Starting Custom Transformation Example");

        SparkSession spark = SparkSession.builder()
                .appName("Custom Transformation Example")
                .master("local[*]")
                .getOrCreate();

        try {
            // Step 1: Register custom transformation
            logger.info("Registering custom transformation...");
            TransformationRegistry registry = TransformationRegistry.getInstance();
            registry.registerTransformation(new MaskSensitiveDataTransform());

            // Create sample data with sensitive information
            createSampleSensitiveData(spark);

            // Step 2: Build pipeline using custom transformation
            PipelineConfig config = PipelineBuilder.create("mask-sensitive-data-pipeline")
                    .fromSource("file", Map.of(
                            "path", "/tmp/sensitive_customer_data.csv",
                            "format", "csv",
                            "options", Map.of("header", "true", "inferSchema", "true")
                    ))
                    // Use our custom transformation
                    .transform("maskSensitiveData", Map.of(
                            "columns", List.of("credit_card", "ssn"),
                            "maskChar", "*",
                            "visibleChars", 4
                    ))
                    // Standard transformations
                    .transform("select", Map.of(
                            "columns", List.of("customer_id", "name", "credit_card", "ssn")
                    ))
                    .toSink("file", Map.of(
                            "path", "/tmp/output/masked_customer_data.parquet",
                            "format", "parquet",
                            "mode", "overwrite"
                    ))
                    .build();

            // Step 3: Execute pipeline
            PipelineEngine engine = new PipelineEngine(spark);
            engine.execute(config);

            // Show results
            logger.info("=== Original Data (before masking) ===");
            spark.read()
                    .option("header", "true")
                    .csv("/tmp/sensitive_customer_data.csv")
                    .show(false);

            logger.info("=== Masked Data (after transformation) ===");
            spark.read()
                    .parquet("/tmp/output/masked_customer_data.parquet")
                    .show(false);

            logger.info("Custom transformation executed successfully!");

        } catch (PipelineException e) {
            logger.error("Pipeline execution failed", e);
            System.exit(1);
        } finally {
            spark.stop();
        }
    }

    /**
     * Create sample data with sensitive information.
     */
    private static void createSampleSensitiveData(SparkSession spark) {
        logger.info("Creating sample sensitive data...");

        String csvContent = """
                customer_id,name,credit_card,ssn,email
                1,John Doe,4532015112830366,123-45-6789,john.doe@example.com
                2,Jane Smith,5425233430109903,987-65-4321,jane.smith@example.com
                3,Bob Johnson,4916338506082832,456-78-9012,bob.johnson@example.com
                4,Alice Williams,4024007134564842,789-01-2345,alice.williams@example.com
                5,Charlie Brown,4485370538571359,234-56-7890,charlie.brown@example.com
                """;

        try {
            java.nio.file.Files.writeString(
                    java.nio.file.Path.of("/tmp/sensitive_customer_data.csv"),
                    csvContent
            );
            logger.info("Sample sensitive data created");
        } catch (Exception e) {
            logger.error("Failed to create sample data", e);
            throw new RuntimeException(e);
        }
    }
}
