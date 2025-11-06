package com.enterprise.pipeline.examples;

import com.enterprise.pipeline.api.PipelineException;
import com.enterprise.pipeline.api.model.PipelineConfig;
import com.enterprise.pipeline.core.config.ConfigLoader;
import com.enterprise.pipeline.core.engine.PipelineEngine;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pipeline example using JSON configuration.
 *
 * This example demonstrates:
 * 1. Loading pipeline config from JSON file
 * 2. Executing pipeline from configuration
 * 3. Configuration-driven pipeline approach
 *
 * @author Enterprise Data Pipeline Team
 */
public class JsonConfigPipelineExample {

    private static final Logger logger = LoggerFactory.getLogger(JsonConfigPipelineExample.class);

    public static void main(String[] args) {
        logger.info("Starting JSON Config Pipeline Example");

        if (args.length < 1) {
            logger.error("Usage: JsonConfigPipelineExample <config-file-path>");
            System.exit(1);
        }

        String configPath = args[0];

        // Create Spark session
        SparkSession spark = SparkSession.builder()
                .appName("JSON Config Pipeline Example")
                .master("local[*]")
                .config("spark.sql.adaptive.enabled", "true")
                .getOrCreate();

        try {
            // Load configuration
            ConfigLoader loader = new ConfigLoader();
            PipelineConfig config = loader.loadFromFile(configPath);

            logger.info("Loaded pipeline configuration: {}", config.name());

            // Execute pipeline
            PipelineEngine engine = new PipelineEngine(spark);
            engine.execute(config);

            logger.info("Pipeline execution completed successfully");

        } catch (PipelineException e) {
            logger.error("Pipeline execution failed", e);
            System.exit(1);
        } finally {
            spark.stop();
        }
    }
}
