package com.enterprise.pipeline.quality.anomaly;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Detects volume anomalies - unexpected row counts.
 */
public class VolumeAnomalyDetector implements AnomalyDetector {
    private static final Logger logger = LoggerFactory.getLogger(VolumeAnomalyDetector.class);

    private final long expectedCount;
    private final double tolerancePercent;

    /**
     * Create detector with expected count and tolerance.
     *
     * @param expectedCount Expected number of rows
     * @param tolerancePercent Acceptable deviation percentage (e.g., 10.0 for 10%)
     */
    public VolumeAnomalyDetector(long expectedCount, double tolerancePercent) {
        this.expectedCount = expectedCount;
        this.tolerancePercent = tolerancePercent;
    }

    @Override
    public String getName() {
        return "Volume Anomaly Detector";
    }

    @Override
    public List<Anomaly> detect(Dataset<Row> dataset) {
        List<Anomaly> anomalies = new ArrayList<>();

        long actualCount = dataset.count();
        double deviation = Math.abs(actualCount - expectedCount);
        double deviationPercent = (deviation / expectedCount) * 100.0;

        if (deviationPercent > tolerancePercent) {
            String direction = actualCount > expectedCount ? "higher" : "lower";
            anomalies.add(new Anomaly(
                "VOLUME_ANOMALY",
                String.format("Row count is %.2f%% %s than expected", deviationPercent, direction),
                "*",
                actualCount,
                deviationPercent,
                String.format("Expected: %d, Actual: %d, Tolerance: %.2f%%",
                    expectedCount, actualCount, tolerancePercent)
            ));

            logger.warn("Volume anomaly detected: expected {}, got {}", expectedCount, actualCount);
        }

        return anomalies;
    }

    /**
     * Create detector with min/max bounds.
     */
    public static VolumeAnomalyDetector withBounds(long minCount, long maxCount) {
        long midpoint = (minCount + maxCount) / 2;
        double tolerance = ((maxCount - minCount) / 2.0 / midpoint) * 100.0;
        return new VolumeAnomalyDetector(midpoint, tolerance);
    }
}
