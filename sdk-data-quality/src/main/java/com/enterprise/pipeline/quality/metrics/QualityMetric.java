package com.enterprise.pipeline.quality.metrics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;

/**
 * Interface for data quality metrics.
 * Each metric evaluates a specific aspect of data quality.
 */
public interface QualityMetric extends Serializable {
    /**
     * Get the name of this metric.
     */
    String getName();

    /**
     * Calculate the metric score (0.0 to 100.0).
     * Higher scores indicate better quality.
     */
    double calculate(Dataset<Row> dataset);

    /**
     * Get a description of what this metric measures.
     */
    String getDescription();

    /**
     * Check if the metric passes a threshold.
     */
    default boolean passes(Dataset<Row> dataset, double threshold) {
        return calculate(dataset) >= threshold;
    }
}
