package com.enterprise.pipeline.api;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

/**
 * Context for transformation execution.
 * Provides access to shared resources and intermediate datasets.
 *
 * Design Principles:
 * - Immutable: Safe for concurrent access
 * - Simple: Easy to understand API
 * - Extensible: Can add metadata as needed
 *
 * @author Enterprise Data Pipeline Team
 */
public interface TransformationContext extends Serializable {

    /**
     * Get the Spark session for this execution.
     *
     * @return SparkSession instance
     */
    SparkSession getSparkSession();

    /**
     * Get a named dataset from the context.
     * Used for joins and multi-input transformations.
     *
     * @param name Dataset name
     * @return Optional containing the dataset if found
     */
    Optional<Dataset<Row>> getDataset(String name);

    /**
     * Register a named dataset in the context.
     * Makes it available for subsequent transformations.
     *
     * @param name Dataset name
     * @param dataset Dataset to register
     */
    void registerDataset(String name, Dataset<Row> dataset);

    /**
     * Get pipeline-level configuration.
     *
     * @return Immutable configuration map
     */
    Map<String, Object> getPipelineConfig();

    /**
     * Get metadata value by key.
     *
     * @param key Metadata key
     * @return Optional containing the value if found
     */
    Optional<Object> getMetadata(String key);

    /**
     * Set metadata value.
     *
     * @param key Metadata key
     * @param value Metadata value
     */
    void setMetadata(String key, Object value);
}
