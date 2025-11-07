package com.enterprise.pipeline.quality.report;

import com.enterprise.pipeline.quality.anomaly.AnomalyDetector;
import com.enterprise.pipeline.quality.metrics.QualityMetrics;
import com.enterprise.pipeline.quality.profiler.ProfileResult;
import com.enterprise.pipeline.quality.rules.RuleEngine;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;

/**
 * Comprehensive data quality report containing all quality assessments.
 */
public class QualityReport implements Serializable {
    private final String datasetName;
    private final Instant generatedAt;
    private final ProfileResult profile;
    private final QualityMetrics.QualityMetricsResult metricsResult;
    private final RuleEngine.RuleExecutionResult rulesResult;
    private final List<AnomalyDetector.Anomaly> anomalies;
    private final double overallScore;

    public QualityReport(String datasetName,
                        ProfileResult profile,
                        QualityMetrics.QualityMetricsResult metricsResult,
                        RuleEngine.RuleExecutionResult rulesResult,
                        List<AnomalyDetector.Anomaly> anomalies) {
        this.datasetName = datasetName;
        this.generatedAt = Instant.now();
        this.profile = profile;
        this.metricsResult = metricsResult;
        this.rulesResult = rulesResult;
        this.anomalies = anomalies;
        this.overallScore = calculateOverallScore();
    }

    private double calculateOverallScore() {
        double profileScore = profile != null ? profile.getOverallCompletenessPercent() : 100.0;
        double metricsScore = metricsResult != null ? metricsResult.getOverallScore() : 100.0;
        double rulesScore = rulesResult != null ?
            (rulesResult.getPassedCount() * 100.0 / rulesResult.getTotalCount()) : 100.0;
        double anomalyPenalty = anomalies != null ? Math.min(anomalies.size() * 5.0, 50.0) : 0.0;

        return ((profileScore + metricsScore + rulesScore) / 3.0) - anomalyPenalty;
    }

    public String getDatasetName() { return datasetName; }
    public Instant getGeneratedAt() { return generatedAt; }
    public ProfileResult getProfile() { return profile; }
    public QualityMetrics.QualityMetricsResult getMetricsResult() { return metricsResult; }
    public RuleEngine.RuleExecutionResult getRulesResult() { return rulesResult; }
    public List<AnomalyDetector.Anomaly> getAnomalies() { return anomalies; }
    public double getOverallScore() { return overallScore; }

    public boolean passes(double threshold) {
        return overallScore >= threshold;
    }

    public String getSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("═".repeat(100)).append("\n");
        sb.append("                       COMPREHENSIVE DATA QUALITY REPORT\n");
        sb.append("═".repeat(100)).append("\n\n");

        sb.append("Dataset: ").append(datasetName).append("\n");
        sb.append("Generated: ").append(generatedAt).append("\n");
        sb.append("Overall Quality Score: ").append(String.format("%.2f%%", overallScore)).append("\n");
        sb.append(getScoreGrade()).append("\n\n");

        sb.append("─".repeat(100)).append("\n");
        sb.append("SECTION 1: DATA PROFILE\n");
        sb.append("─".repeat(100)).append("\n");
        if (profile != null) {
            sb.append("Total Rows: ").append(profile.getTotalRows()).append("\n");
            sb.append("Total Columns: ").append(profile.getTotalColumns()).append("\n");
            sb.append("Completeness: ").append(String.format("%.2f%%", profile.getOverallCompletenessPercent())).append("\n");
            sb.append("Columns with Nulls: ").append(profile.getColumnsWithNulls()).append("\n");
            sb.append("Columns with Duplicates: ").append(profile.getColumnsWithDuplicates()).append("\n");
        } else {
            sb.append("No profiling data available\n");
        }
        sb.append("\n");

        sb.append("─".repeat(100)).append("\n");
        sb.append("SECTION 2: QUALITY METRICS\n");
        sb.append("─".repeat(100)).append("\n");
        if (metricsResult != null) {
            sb.append(metricsResult.getSummary());
        } else {
            sb.append("No metrics data available\n");
        }
        sb.append("\n");

        sb.append("─".repeat(100)).append("\n");
        sb.append("SECTION 3: QUALITY RULES\n");
        sb.append("─".repeat(100)).append("\n");
        if (rulesResult != null) {
            sb.append("Total Rules: ").append(rulesResult.getTotalCount()).append("\n");
            sb.append("Passed: ").append(rulesResult.getPassedCount()).append("\n");
            sb.append("Failed: ").append(rulesResult.getFailedCount()).append("\n");
            sb.append("Total Violations: ").append(rulesResult.getTotalViolations()).append("\n");

            if (!rulesResult.getViolations().isEmpty()) {
                sb.append("\nTop Violations:\n");
                rulesResult.getViolations().entrySet().stream()
                    .limit(5)
                    .forEach(entry -> {
                        sb.append("  - ").append(entry.getKey())
                          .append(": ").append(entry.getValue().getViolationCount())
                          .append(" violations\n");
                    });
            }
        } else {
            sb.append("No rules data available\n");
        }
        sb.append("\n");

        sb.append("─".repeat(100)).append("\n");
        sb.append("SECTION 4: ANOMALIES\n");
        sb.append("─".repeat(100)).append("\n");
        if (anomalies != null && !anomalies.isEmpty()) {
            sb.append("Total Anomalies Detected: ").append(anomalies.size()).append("\n\n");
            anomalies.stream()
                .limit(10)
                .forEach(anomaly -> sb.append("  ").append(anomaly.toString()).append("\n"));

            if (anomalies.size() > 10) {
                sb.append("\n  ... and ").append(anomalies.size() - 10).append(" more\n");
            }
        } else {
            sb.append("No anomalies detected\n");
        }
        sb.append("\n");

        sb.append("═".repeat(100)).append("\n");
        sb.append("END OF REPORT\n");
        sb.append("═".repeat(100)).append("\n");

        return sb.toString();
    }

    private String getScoreGrade() {
        if (overallScore >= 90) return "Grade: A (Excellent)";
        if (overallScore >= 80) return "Grade: B (Good)";
        if (overallScore >= 70) return "Grade: C (Fair)";
        if (overallScore >= 60) return "Grade: D (Poor)";
        return "Grade: F (Critical Issues)";
    }

    @Override
    public String toString() {
        return getSummary();
    }
}
