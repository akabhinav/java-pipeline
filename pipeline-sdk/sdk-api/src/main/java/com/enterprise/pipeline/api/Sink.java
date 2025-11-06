package com.enterprise.pipeline.api;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.Map;

/**
 * Interface for data sinks.
 * Writes data from Spark DataFrames to external systems.
 *
 * Design Principles:
 * - Simple: Single write method
 * - Extensible: Easy to add new sinks
 * - Configurable: Map-based configuration
 *
 * Built-in sinks:
 * - JDBC (databases)
 * - Files (CSV, JSON, Parquet, Avro)
 * - S3
 * - Kafka
 *
 * @author Enterprise Data Pipeline Team
 */
public interface Sink extends Serializable {

    /**
     * Write data to the sink.
     *
     * @param dataset Data to write
     * @param config Sink configuration (connection details, format, mode, etc.)
     * @param context Execution context
     * @throws SinkException if writing fails
     */
    void write(Dataset<Row> dataset, Map<String, Object> config, TransformationContext context)
            throws SinkException;

    /**
     * Get the name of this sink.
     *
     * @return Sink name (e.g., "jdbc", "file", "kafka")
     */
    String getName();

    /**
     * Validate sink configuration.
     *
     * @param config Configuration to validate
     * @throws ValidationException if configuration is invalid
     */
    default void validate(Map<String, Object> config) throws ValidationException {
        // Default: no validation
    }
}
