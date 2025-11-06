package com.enterprise.pipeline.api;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.Map;

/**
 * Interface for data sources.
 * Reads data from external systems into Spark DataFrames.
 *
 * Design Principles:
 * - Simple: Single read method
 * - Extensible: Easy to add new sources
 * - Configurable: Map-based configuration
 *
 * Built-in sources:
 * - JDBC (databases)
 * - Files (CSV, JSON, Parquet, Avro)
 * - S3
 * - Kafka
 *
 * @author Enterprise Data Pipeline Team
 */
public interface Source extends Serializable {

    /**
     * Read data from the source.
     *
     * @param config Source configuration (connection details, format, etc.)
     * @param context Execution context
     * @return Dataset containing the read data
     * @throws SourceException if reading fails
     */
    Dataset<Row> read(Map<String, Object> config, TransformationContext context)
            throws SourceException;

    /**
     * Get the name of this source.
     *
     * @return Source name (e.g., "jdbc", "file", "kafka")
     */
    String getName();

    /**
     * Validate source configuration.
     *
     * @param config Configuration to validate
     * @throws ValidationException if configuration is invalid
     */
    default void validate(Map<String, Object> config) throws ValidationException {
        // Default: no validation
    }
}
