package com.enterprise.pipeline.core.engine;

import com.enterprise.pipeline.api.*;
import com.enterprise.pipeline.api.model.PipelineConfig;
import com.enterprise.pipeline.api.model.TransformationConfig;
import com.enterprise.pipeline.core.context.DefaultTransformationContext;
import com.enterprise.pipeline.core.registry.TransformationRegistry;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Core pipeline execution engine.
 *
 * Orchestrates the pipeline execution:
 * 1. Read from source
 * 2. Apply transformations in sequence
 * 3. Write to sink
 *
 * Design Principles:
 * - Simple: Linear execution flow
 * - Clear: Logs each step
 * - Fail-fast: Validates before execution
 *
 * @author Enterprise Data Pipeline Team
 */
public class PipelineEngine {

    private static final Logger logger = LoggerFactory.getLogger(PipelineEngine.class);

    private final SparkSession sparkSession;
    private final TransformationRegistry registry;

    public PipelineEngine(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
        this.registry = TransformationRegistry.getInstance();
    }

    /**
     * Execute a pipeline from configuration.
     *
     * @param config Pipeline configuration
     * @throws PipelineException if execution fails
     */
    public void execute(PipelineConfig config) throws PipelineException {
        logger.info("Starting pipeline execution: {}", config.name());

        // Create context
        Map<String, Object> settings = config.settings() != null ? config.settings() : Map.of();
        TransformationContext context = new DefaultTransformationContext(sparkSession, settings);

        try {
            // Step 1: Read from source
            Dataset<Row> dataset = readFromSource(config, context);
            logger.info("Source read complete. Dataset schema:");
            dataset.printSchema();

            // Register source dataset if named
            if (config.source().name() != null && !config.source().name().isBlank()) {
                context.registerDataset(config.source().name(), dataset);
            }

            // Step 2: Apply transformations
            dataset = applyTransformations(dataset, config, context);

            // Step 3: Write to sink
            writeToSink(dataset, config, context);

            logger.info("Pipeline execution complete: {}", config.name());

        } catch (PipelineException e) {
            logger.error("Pipeline execution failed: {}", config.name(), e);
            throw e;
        } catch (Exception e) {
            logger.error("Unexpected error in pipeline execution: {}", config.name(), e);
            throw new PipelineException("Unexpected error in pipeline: " + e.getMessage(), e);
        }
    }

    /**
     * Read data from the configured source.
     */
    private Dataset<Row> readFromSource(PipelineConfig config, TransformationContext context)
            throws PipelineException {
        logger.info("Reading from source: {}", config.source().type());

        Source source = registry.getSource(config.source().type())
                .orElseThrow(() -> new PipelineException(
                        "Unknown source type: " + config.source().type()));

        // Validate source configuration
        source.validate(config.source().config());

        // Read data
        try {
            return source.read(config.source().config(), context);
        } catch (SourceException e) {
            throw e;
        } catch (Exception e) {
            throw new SourceException(config.source().type(),
                    "Failed to read from source: " + e.getMessage(), e);
        }
    }

    /**
     * Apply all transformations in sequence.
     */
    private Dataset<Row> applyTransformations(Dataset<Row> dataset, PipelineConfig config,
                                               TransformationContext context) throws PipelineException {
        logger.info("Applying {} transformations", config.transformations().size());

        int step = 1;
        for (TransformationConfig transformConfig : config.transformations()) {
            logger.info("Step {}: Applying transformation '{}'", step, transformConfig.type());

            Transformation transformation = registry.getTransformation(transformConfig.type())
                    .orElseThrow(() -> new PipelineException(
                            "Unknown transformation type: " + transformConfig.type()));

            // Validate transformation configuration
            transformation.validate(transformConfig.config());

            // Apply transformation
            try {
                dataset = transformation.transform(dataset, transformConfig.config(), context);

                // Register transformed dataset if named
                if (transformConfig.name() != null && !transformConfig.name().isBlank()) {
                    context.registerDataset(transformConfig.name(), dataset);
                }

                logger.debug("Transformation complete. New schema:");
                dataset.printSchema();

            } catch (TransformationException e) {
                throw e;
            } catch (Exception e) {
                throw new TransformationException(transformConfig.type(),
                        "Failed to apply transformation: " + e.getMessage(), e);
            }

            step++;
        }

        return dataset;
    }

    /**
     * Write data to the configured sink.
     */
    private void writeToSink(Dataset<Row> dataset, PipelineConfig config, TransformationContext context)
            throws PipelineException {
        logger.info("Writing to sink: {}", config.sink().type());

        Sink sink = registry.getSink(config.sink().type())
                .orElseThrow(() -> new PipelineException(
                        "Unknown sink type: " + config.sink().type()));

        // Validate sink configuration
        sink.validate(config.sink().config());

        // Write data
        try {
            sink.write(dataset, config.sink().config(), context);
        } catch (SinkException e) {
            throw e;
        } catch (Exception e) {
            throw new SinkException(config.sink().type(),
                    "Failed to write to sink: " + e.getMessage(), e);
        }
    }
}
