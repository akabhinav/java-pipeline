package com.enterprise.pipeline.api;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.Map;

/**
 * Core interface for all data transformations in the pipeline.
 *
 * Design Principles:
 * - Simple: Single method to implement
 * - Extensible: Easy to add new transformations
 * - Testable: Pure function transformation
 *
 * Usage:
 * Implement this interface to create custom transformations.
 * Configure via Map<String, Object> for flexibility.
 *
 * @author Enterprise Data Pipeline Team
 */
public interface Transformation extends Serializable {

    /**
     * Transform input dataset to output dataset.
     *
     * @param input Input dataset to transform
     * @param config Configuration parameters for this transformation
     * @param context Execution context with shared resources
     * @return Transformed dataset
     * @throws TransformationException if transformation fails
     */
    Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException;

    /**
     * Get the name of this transformation.
     * Used for registration and discovery.
     *
     * @return Transformation name (e.g., "select", "filter", "join")
     */
    String getName();

    /**
     * Validate configuration before execution.
     * Throw exception if config is invalid.
     *
     * @param config Configuration to validate
     * @throws ValidationException if configuration is invalid
     */
    default void validate(Map<String, Object> config) throws ValidationException {
        // Default: no validation
    }
}
