package com.enterprise.pipeline.quality.metrics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.*;

/**
 * Container for multiple quality metrics with aggregated scoring.
 */
public class QualityMetrics implements Serializable {
    private final List<QualityMetric> metrics;
    private final Map<String, Double> weights;

    private QualityMetrics(Builder builder) {
        this.metrics = builder.metrics;
        this.weights = builder.weights;
    }

    /**
     * Calculate all metrics and return results.
     */
    public QualityMetricsResult calculate(Dataset<Row> dataset) {
        Map<String, Double> scores = new LinkedHashMap<>();

        for (QualityMetric metric : metrics) {
            double score = metric.calculate(dataset);
            scores.put(metric.getName(), score);
        }

        double overallScore = calculateWeightedScore(scores);

        return new QualityMetricsResult(scores, overallScore);
    }

    /**
     * Calculate weighted overall score.
     */
    private double calculateWeightedScore(Map<String, Double> scores) {
        if (scores.isEmpty()) {
            return 100.0;
        }

        if (weights.isEmpty()) {
            // Equal weights
            return scores.values().stream()
                .mapToDouble(Double::doubleValue)
                .average()
                .orElse(100.0);
        }

        // Weighted average
        double weightedSum = 0.0;
        double totalWeight = 0.0;

        for (Map.Entry<String, Double> entry : scores.entrySet()) {
            String metricName = entry.getKey();
            double score = entry.getValue();
            double weight = weights.getOrDefault(metricName, 1.0);

            weightedSum += score * weight;
            totalWeight += weight;
        }

        return totalWeight > 0 ? weightedSum / totalWeight : 100.0;
    }

    /**
     * Check if all metrics pass their thresholds.
     */
    public boolean allPass(Dataset<Row> dataset, double threshold) {
        return metrics.stream()
            .allMatch(metric -> metric.passes(dataset, threshold));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final List<QualityMetric> metrics = new ArrayList<>();
        private final Map<String, Double> weights = new HashMap<>();

        public Builder add(QualityMetric metric) {
            metrics.add(metric);
            return this;
        }

        public Builder add(QualityMetric metric, double weight) {
            metrics.add(metric);
            weights.put(metric.getName(), weight);
            return this;
        }

        public Builder addCompleteness(String columnName) {
            return add(new CompletenessMetric(columnName));
        }

        public Builder addUniqueness(String... columns) {
            return add(new UniquenessMetric(columns));
        }

        public Builder addAccuracy(String columnName, String sqlExpression) {
            return add(AccuracyMetric.withExpression(columnName, sqlExpression));
        }

        public Builder addConsistency(String sqlExpression) {
            return add(ConsistencyMetric.withExpression(sqlExpression));
        }

        public QualityMetrics build() {
            return new QualityMetrics(this);
        }
    }

    /**
     * Result of quality metrics calculation.
     */
    public static class QualityMetricsResult implements Serializable {
        private final Map<String, Double> metricScores;
        private final double overallScore;

        public QualityMetricsResult(Map<String, Double> metricScores, double overallScore) {
            this.metricScores = metricScores;
            this.overallScore = overallScore;
        }

        public Map<String, Double> getMetricScores() {
            return metricScores;
        }

        public double getOverallScore() {
            return overallScore;
        }

        public double getScore(String metricName) {
            return metricScores.getOrDefault(metricName, 0.0);
        }

        public boolean passes(double threshold) {
            return overallScore >= threshold;
        }

        public String getSummary() {
            StringBuilder sb = new StringBuilder();
            sb.append("Quality Metrics Summary:\n");
            sb.append("=".repeat(60)).append("\n");

            for (Map.Entry<String, Double> entry : metricScores.entrySet()) {
                sb.append(String.format("%-40s: %6.2f%%\n",
                    entry.getKey(), entry.getValue()));
            }

            sb.append("=".repeat(60)).append("\n");
            sb.append(String.format("%-40s: %6.2f%%\n",
                "OVERALL QUALITY SCORE", overallScore));
            sb.append("=".repeat(60)).append("\n");

            return sb.toString();
        }

        @Override
        public String toString() {
            return getSummary();
        }
    }
}
