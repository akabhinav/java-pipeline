package com.enterprise.pipeline.quality.anomaly;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * Detects statistical outliers using Z-score and IQR methods.
 */
public class StatisticalAnomalyDetector implements AnomalyDetector {
    private static final Logger logger = LoggerFactory.getLogger(StatisticalAnomalyDetector.class);

    private final double zScoreThreshold;
    private final double iqrMultiplier;
    private final Method method;

    public enum Method {
        Z_SCORE,    // Standard deviations from mean
        IQR,        // Interquartile range
        BOTH        // Both methods
    }

    public StatisticalAnomalyDetector() {
        this(3.0, 1.5, Method.BOTH);
    }

    public StatisticalAnomalyDetector(double zScoreThreshold, double iqrMultiplier, Method method) {
        this.zScoreThreshold = zScoreThreshold;
        this.iqrMultiplier = iqrMultiplier;
        this.method = method;
    }

    @Override
    public String getName() {
        return "Statistical Outlier Detector";
    }

    @Override
    public List<Anomaly> detect(Dataset<Row> dataset) {
        List<Anomaly> anomalies = new ArrayList<>();

        // Check each numeric column
        for (StructField field : dataset.schema().fields()) {
            if (isNumericType(field.dataType())) {
                anomalies.addAll(detectInColumn(dataset, field.name()));
            }
        }

        logger.info("Detected {} statistical anomalies", anomalies.size());
        return anomalies;
    }

    private List<Anomaly> detectInColumn(Dataset<Row> dataset, String columnName) {
        List<Anomaly> anomalies = new ArrayList<>();

        // Calculate statistics
        Dataset<Row> stats = dataset.select(
            avg(columnName).alias("mean"),
            stddev(columnName).alias("stddev")
        );

        Row statsRow = stats.first();
        Double mean = statsRow.isNullAt(0) ? null : statsRow.getDouble(0);
        Double stddev = statsRow.isNullAt(1) ? null : statsRow.getDouble(1);

        if (mean == null || stddev == null || stddev == 0) {
            return anomalies; // Can't detect outliers
        }

        // Z-score method
        if (method == Method.Z_SCORE || method == Method.BOTH) {
            double lowerBound = mean - (zScoreThreshold * stddev);
            double upperBound = mean + (zScoreThreshold * stddev);

            Dataset<Row> outliers = dataset
                .filter(col(columnName).isNotNull())
                .filter(col(columnName).lt(lowerBound).or(col(columnName).gt(upperBound)))
                .select(columnName)
                .limit(100); // Limit to first 100 outliers

            List<Row> outlierRows = outliers.collectAsList();
            for (Row row : outlierRows) {
                double value = row.getDouble(0);
                double zScore = Math.abs((value - mean) / stddev);

                anomalies.add(new Anomaly(
                    "STATISTICAL_OUTLIER",
                    "Value is " + String.format("%.2f", zScore) + " standard deviations from mean",
                    columnName,
                    value,
                    zScore,
                    String.format("Mean: %.2f, StdDev: %.2f, Z-Score: %.2f",
                        mean, stddev, zScore)
                ));
            }
        }

        // IQR method
        if (method == Method.IQR || method == Method.BOTH) {
            try {
                double[] quantiles = dataset.stat().approxQuantile(
                    columnName, new double[]{0.25, 0.75}, 0.01);

                if (quantiles != null && quantiles.length == 2) {
                    double q1 = quantiles[0];
                    double q3 = quantiles[1];
                    double iqr = q3 - q1;
                    double lowerBound = q1 - (iqrMultiplier * iqr);
                    double upperBound = q3 + (iqrMultiplier * iqr);

                    Dataset<Row> outliers = dataset
                        .filter(col(columnName).isNotNull())
                        .filter(col(columnName).lt(lowerBound).or(col(columnName).gt(upperBound)))
                        .select(columnName)
                        .limit(100);

                    List<Row> outlierRows = outliers.collectAsList();
                    for (Row row : outlierRows) {
                        double value = row.getDouble(0);

                        anomalies.add(new Anomaly(
                            "IQR_OUTLIER",
                            "Value outside IQR bounds",
                            columnName,
                            value,
                            Math.max(
                                Math.abs(value - lowerBound),
                                Math.abs(value - upperBound)
                            ) / iqr,
                            String.format("Q1: %.2f, Q3: %.2f, IQR: %.2f, Bounds: [%.2f, %.2f]",
                                q1, q3, iqr, lowerBound, upperBound)
                        ));
                    }
                }
            } catch (Exception e) {
                logger.warn("Could not calculate IQR for column: {}", columnName, e);
            }
        }

        return anomalies;
    }

    private boolean isNumericType(org.apache.spark.sql.types.DataType dataType) {
        return dataType == DataTypes.IntegerType ||
               dataType == DataTypes.LongType ||
               dataType == DataTypes.DoubleType ||
               dataType == DataTypes.FloatType ||
               dataType.toString().contains("DecimalType");
    }
}
