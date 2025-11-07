package com.enterprise.pipeline.quality.anomaly;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.List;

/**
 * Interface for anomaly detection algorithms.
 */
public interface AnomalyDetector extends Serializable {
    /**
     * Detect anomalies in the dataset.
     *
     * @return List of detected anomalies
     */
    List<Anomaly> detect(Dataset<Row> dataset);

    /**
     * Get detector name.
     */
    String getName();

    /**
     * Represents a detected anomaly.
     */
    class Anomaly implements Serializable {
        private final String type;
        private final String description;
        private final String columnName;
        private final Object value;
        private final double score; // Anomaly score (higher = more anomalous)
        private final String details;

        public Anomaly(String type, String description, String columnName,
                      Object value, double score, String details) {
            this.type = type;
            this.description = description;
            this.columnName = columnName;
            this.value = value;
            this.score = score;
            this.details = details;
        }

        public String getType() { return type; }
        public String getDescription() { return description; }
        public String getColumnName() { return columnName; }
        public Object getValue() { return value; }
        public double getScore() { return score; }
        public String getDetails() { return details; }

        @Override
        public String toString() {
            return String.format("[%s] %s (column: %s, value: %s, score: %.2f) - %s",
                type, description, columnName, value, score, details);
        }
    }
}
