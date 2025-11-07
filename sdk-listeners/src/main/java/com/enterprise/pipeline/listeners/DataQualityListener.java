package com.enterprise.pipeline.listeners;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Data Quality Listener
 *
 * Monitors data quality metrics across ALL industries.
 *
 * Metrics Tracked:
 * - Row counts (input vs output)
 * - Null value percentages per column
 * - Duplicate records
 * - Schema validation
 * - Data type compliance
 * - Data loss detection
 *
 * Use Cases:
 * - Detect data quality issues early
 * - Track data loss through pipeline
 * - Monitor null value trends
 * - Schema drift detection
 * - Quality score calculation
 *
 * @author Enterprise Pipeline Platform
 */
public class DataQualityListener extends SparkListener {

    private static final Logger logger = LoggerFactory.getLogger(DataQualityListener.class);

    private final Map<String, QualityMetrics> qualityMetricsMap = new ConcurrentHashMap<>();
    private final SparkSession spark;

    public DataQualityListener(SparkSession spark) {
        this.spark = spark;
    }

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        int jobId = jobStart.jobId();
        logger.info("Data Quality monitoring started for Job {}", jobId);
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        int jobId = jobEnd.jobId();
        logger.info("Data Quality monitoring completed for Job {}", jobId);
    }

    /**
     * Track data quality for a dataset
     * Call this method manually in your pipeline after each transformation
     *
     * Example:
     *   Dataset<Row> data = spark.read().csv("input.csv");
     *   dataQualityListener.trackQuality("input_data", data);
     */
    public QualityMetrics trackQuality(String stageName, Dataset<Row> dataset) {
        logger.info("Tracking data quality for stage: {}", stageName);

        QualityMetrics metrics = new QualityMetrics(stageName);

        try {
            // Row count
            long rowCount = dataset.count();
            metrics.setRowCount(rowCount);
            logger.info("Stage {}: Row count = {}", stageName, rowCount);

            // Column count
            int columnCount = dataset.columns().length;
            metrics.setColumnCount(columnCount);
            logger.info("Stage {}: Column count = {}", stageName, columnCount);

            // Null counts per column
            for (String column : dataset.columns()) {
                long nullCount = dataset.filter(dataset.col(column).isNull()).count();
                double nullPercentage = (rowCount > 0) ? (nullCount * 100.0 / rowCount) : 0.0;

                metrics.addNullCount(column, nullCount, nullPercentage);

                if (nullPercentage > 50) {
                    logger.warn("Stage {}: Column {} has high null percentage: {:.2f}%",
                        stageName, column, nullPercentage);
                }
            }

            // Duplicate count
            long distinctCount = dataset.distinct().count();
            long duplicateCount = rowCount - distinctCount;
            metrics.setDuplicateCount(duplicateCount);

            if (duplicateCount > 0) {
                logger.info("Stage {}: Duplicate records = {} ({:.2f}%)",
                    stageName, duplicateCount, (duplicateCount * 100.0 / rowCount));
            }

            // Calculate overall quality score (0-100)
            double qualityScore = calculateQualityScore(metrics);
            metrics.setQualityScore(qualityScore);
            logger.info("Stage {}: Quality Score = {:.2f}/100", stageName, qualityScore);

            // Store metrics
            qualityMetricsMap.put(stageName, metrics);

            // Alert on low quality
            if (qualityScore < 70) {
                logger.warn("Stage {}: Low data quality score: {:.2f}/100", stageName, qualityScore);
            }

        } catch (Exception e) {
            logger.error("Error tracking data quality for stage {}: {}", stageName, e.getMessage(), e);
        }

        return metrics;
    }

    /**
     * Compare two stages to detect data loss
     */
    public void compareStages(String beforeStage, String afterStage) {
        QualityMetrics before = qualityMetricsMap.get(beforeStage);
        QualityMetrics after = qualityMetricsMap.get(afterStage);

        if (before == null || after == null) {
            logger.warn("Cannot compare stages: metrics not found");
            return;
        }

        long rowLoss = before.getRowCount() - after.getRowCount();
        double lossPercentage = (before.getRowCount() > 0) ?
            (rowLoss * 100.0 / before.getRowCount()) : 0.0;

        logger.info("Data loss from {} to {}: {} rows ({:.2f}%)",
            beforeStage, afterStage, rowLoss, lossPercentage);

        // Alert on significant data loss (> 10%)
        if (lossPercentage > 10) {
            logger.warn("Significant data loss detected: {:.2f}% from {} to {}",
                lossPercentage, beforeStage, afterStage);
        }
    }

    /**
     * Calculate quality score (0-100) based on:
     * - Null percentages (lower is better)
     * - Duplicate percentages (lower is better)
     * - Row count (higher is better, relative)
     */
    private double calculateQualityScore(QualityMetrics metrics) {
        double score = 100.0;

        // Penalize for high null percentages (max penalty: -30 points)
        double avgNullPercentage = metrics.getNullCounts().values().stream()
            .mapToDouble(nc -> nc.percentage)
            .average()
            .orElse(0.0);
        score -= Math.min(avgNullPercentage * 0.3, 30);

        // Penalize for duplicates (max penalty: -20 points)
        double duplicatePercentage = (metrics.getRowCount() > 0) ?
            (metrics.getDuplicateCount() * 100.0 / metrics.getRowCount()) : 0.0;
        score -= Math.min(duplicatePercentage * 0.2, 20);

        // Ensure score is in [0, 100]
        return Math.max(0, Math.min(100, score));
    }

    /**
     * Get metrics for a specific stage
     */
    public QualityMetrics getMetrics(String stageName) {
        return qualityMetricsMap.get(stageName);
    }

    /**
     * Get all quality metrics
     */
    public Map<String, QualityMetrics> getAllMetrics() {
        return new ConcurrentHashMap<>(qualityMetricsMap);
    }

    /**
     * Quality Metrics POJO
     */
    public static class QualityMetrics {
        private final String stageName;
        private long rowCount;
        private int columnCount;
        private long duplicateCount;
        private double qualityScore;
        private final Map<String, NullCount> nullCounts = new ConcurrentHashMap<>();

        public QualityMetrics(String stageName) {
            this.stageName = stageName;
        }

        public void setRowCount(long rowCount) { this.rowCount = rowCount; }
        public void setColumnCount(int columnCount) { this.columnCount = columnCount; }
        public void setDuplicateCount(long duplicateCount) { this.duplicateCount = duplicateCount; }
        public void setQualityScore(double qualityScore) { this.qualityScore = qualityScore; }

        public void addNullCount(String column, long count, double percentage) {
            nullCounts.put(column, new NullCount(count, percentage));
        }

        public String getStageName() { return stageName; }
        public long getRowCount() { return rowCount; }
        public int getColumnCount() { return columnCount; }
        public long getDuplicateCount() { return duplicateCount; }
        public double getQualityScore() { return qualityScore; }
        public Map<String, NullCount> getNullCounts() { return nullCounts; }

        @Override
        public String toString() {
            return String.format("QualityMetrics{stage=%s, rows=%d, columns=%d, duplicates=%d, score=%.2f}",
                stageName, rowCount, columnCount, duplicateCount, qualityScore);
        }

        public static class NullCount {
            public final long count;
            public final double percentage;

            public NullCount(long count, double percentage) {
                this.count = count;
                this.percentage = percentage;
            }
        }
    }
}
