package com.enterprise.pipeline.quality;

import com.enterprise.pipeline.quality.anomaly.AnomalyDetector;
import com.enterprise.pipeline.quality.anomaly.StatisticalAnomalyDetector;
import com.enterprise.pipeline.quality.anomaly.VolumeAnomalyDetector;
import com.enterprise.pipeline.quality.metrics.QualityMetrics;
import com.enterprise.pipeline.quality.profiler.DataProfiler;
import com.enterprise.pipeline.quality.profiler.ProfileResult;
import com.enterprise.pipeline.quality.report.QualityReport;
import com.enterprise.pipeline.quality.rules.RuleEngine;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Main API for the Data Quality Framework.
 * Provides a fluent interface for comprehensive data quality assessment.
 *
 * <p>Example usage:
 * <pre>{@code
 * QualityReport report = DataQualityFramework.assess(dataset, "customer_data")
 *     .profile()
 *     .withMetrics(metrics -> metrics
 *         .addCompleteness("email")
 *         .addUniqueness("customer_id")
 *     )
 *     .withRules(rules -> rules
 *         .addNotNull("customer_id")
 *         .addRange("age", 18, 120)
 *     )
 *     .detectAnomalies()
 *     .generate();
 *
 * System.out.println(report);
 * }</pre>
 */
public class DataQualityFramework {
    private static final Logger logger = LoggerFactory.getLogger(DataQualityFramework.class);

    private final Dataset<Row> dataset;
    private final String datasetName;

    private boolean enableProfiling = false;
    private QualityMetrics qualityMetrics;
    private RuleEngine ruleEngine;
    private final List<AnomalyDetector> anomalyDetectors = new ArrayList<>();

    private DataQualityFramework(Dataset<Row> dataset, String datasetName) {
        this.dataset = dataset;
        this.datasetName = datasetName;
    }

    /**
     * Start quality assessment for a dataset.
     */
    public static DataQualityFramework assess(Dataset<Row> dataset, String datasetName) {
        return new DataQualityFramework(dataset, datasetName);
    }

    /**
     * Enable data profiling (statistics, distributions, etc.).
     */
    public DataQualityFramework profile() {
        this.enableProfiling = true;
        return this;
    }

    /**
     * Add quality metrics using a builder.
     */
    public DataQualityFramework withMetrics(java.util.function.Consumer<QualityMetrics.Builder> configurator) {
        QualityMetrics.Builder builder = QualityMetrics.builder();
        configurator.accept(builder);
        this.qualityMetrics = builder.build();
        return this;
    }

    /**
     * Add quality rules using a builder.
     */
    public DataQualityFramework withRules(java.util.function.Consumer<RuleEngine.Builder> configurator) {
        RuleEngine.Builder builder = RuleEngine.builder();
        configurator.accept(builder);
        this.ruleEngine = builder.build();
        return this;
    }

    /**
     * Enable anomaly detection with default detectors.
     */
    public DataQualityFramework detectAnomalies() {
        anomalyDetectors.add(new StatisticalAnomalyDetector());
        return this;
    }

    /**
     * Add a custom anomaly detector.
     */
    public DataQualityFramework withAnomalyDetector(AnomalyDetector detector) {
        anomalyDetectors.add(detector);
        return this;
    }

    /**
     * Add volume anomaly detection.
     */
    public DataQualityFramework detectVolumeAnomalies(long expectedCount, double tolerancePercent) {
        anomalyDetectors.add(new VolumeAnomalyDetector(expectedCount, tolerancePercent));
        return this;
    }

    /**
     * Generate the comprehensive quality report.
     */
    public QualityReport generate() {
        logger.info("Starting data quality assessment for dataset: {}", datasetName);

        // 1. Profiling
        ProfileResult profileResult = null;
        if (enableProfiling) {
            logger.info("Profiling dataset...");
            DataProfiler profiler = new DataProfiler();
            profileResult = profiler.profile(dataset, datasetName);
        }

        // 2. Quality Metrics
        QualityMetrics.QualityMetricsResult metricsResult = null;
        if (qualityMetrics != null) {
            logger.info("Calculating quality metrics...");
            metricsResult = qualityMetrics.calculate(dataset);
        }

        // 3. Quality Rules
        RuleEngine.RuleExecutionResult rulesResult = null;
        if (ruleEngine != null) {
            logger.info("Executing quality rules...");
            rulesResult = ruleEngine.execute(dataset);
        }

        // 4. Anomaly Detection
        List<AnomalyDetector.Anomaly> allAnomalies = new ArrayList<>();
        if (!anomalyDetectors.isEmpty()) {
            logger.info("Running anomaly detection...");
            for (AnomalyDetector detector : anomalyDetectors) {
                logger.debug("Running detector: {}", detector.getName());
                List<AnomalyDetector.Anomaly> anomalies = detector.detect(dataset);
                allAnomalies.addAll(anomalies);
            }
        }

        logger.info("Quality assessment complete");

        return new QualityReport(datasetName, profileResult, metricsResult,
                                rulesResult, allAnomalies);
    }

    /**
     * Quick quality check - returns true if data passes threshold.
     */
    public boolean quickCheck(double threshold) {
        QualityReport report = generate();
        return report.passes(threshold);
    }

    /**
     * Convenience method for complete assessment with defaults.
     */
    public static QualityReport completeAssessment(Dataset<Row> dataset, String datasetName) {
        return DataQualityFramework.assess(dataset, datasetName)
            .profile()
            .withMetrics(metrics -> {
                // Add default metrics for all columns
                for (String column : dataset.columns()) {
                    metrics.addCompleteness(column);
                }
            })
            .detectAnomalies()
            .generate();
    }
}
