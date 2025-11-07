package com.enterprise.pipeline.quality.profiler;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

/**
 * Data profiler that analyzes datasets and generates comprehensive profiles.
 * Computes statistics, null counts, distinct values, distributions, and more.
 */
public class DataProfiler {
    private static final Logger logger = LoggerFactory.getLogger(DataProfiler.class);
    private static final int DEFAULT_TOP_VALUES = 10;

    private final int topValuesCount;

    public DataProfiler() {
        this(DEFAULT_TOP_VALUES);
    }

    public DataProfiler(int topValuesCount) {
        this.topValuesCount = topValuesCount;
    }

    /**
     * Profile a dataset and return comprehensive statistics.
     */
    public ProfileResult profile(Dataset<Row> dataset, String datasetName) {
        logger.info("Starting data profiling for dataset: {}", datasetName);

        long totalRows = dataset.count();
        logger.info("Total rows: {}", totalRows);

        List<ColumnProfile> columnProfiles = new ArrayList<>();

        for (StructField field : dataset.schema().fields()) {
            String columnName = field.name();
            String dataType = field.dataType().typeName();

            logger.debug("Profiling column: {} ({})", columnName, dataType);

            ColumnProfile profile = profileColumn(dataset, columnName, dataType, totalRows);
            columnProfiles.add(profile);
        }

        logger.info("Completed data profiling for dataset: {}", datasetName);
        return new ProfileResult(datasetName, totalRows, columnProfiles);
    }

    /**
     * Profile a single column.
     */
    private ColumnProfile profileColumn(Dataset<Row> dataset, String columnName,
                                       String dataType, long totalRows) {
        ColumnProfile.Builder builder = ColumnProfile.builder(columnName)
            .dataType(dataType)
            .totalCount(totalRows);

        // Calculate null count
        long nullCount = dataset.filter(col(columnName).isNull()).count();
        builder.nullCount(nullCount);

        // Calculate distinct count
        long distinctCount = dataset.select(columnName).distinct().count();
        builder.distinctCount(distinctCount);

        // Type-specific profiling
        if (isNumericType(dataType)) {
            profileNumericColumn(dataset, columnName, builder);
        } else if (isStringType(dataType)) {
            profileStringColumn(dataset, columnName, builder);
        }

        // Get value distribution (top N values)
        Map<String, Long> distribution = getValueDistribution(dataset, columnName);
        builder.valueDistribution(distribution);

        return builder.build();
    }

    /**
     * Profile numeric column with statistical measures.
     */
    private void profileNumericColumn(Dataset<Row> dataset, String columnName,
                                     ColumnProfile.Builder builder) {
        Dataset<Row> stats = dataset.select(
            min(columnName).alias("min"),
            max(columnName).alias("max"),
            avg(columnName).alias("mean"),
            stddev(columnName).alias("stddev")
        );

        Row statsRow = stats.first();

        Object minObj = statsRow.get(0);
        Object maxObj = statsRow.get(1);
        Object meanObj = statsRow.get(2);
        Object stddevObj = statsRow.get(3);

        builder.min(minObj != null ? ((Number) minObj).doubleValue() : null);
        builder.max(maxObj != null ? ((Number) maxObj).doubleValue() : null);
        builder.mean(meanObj != null ? ((Number) meanObj).doubleValue() : null);
        builder.stdDev(stddevObj != null ? ((Number) stddevObj).doubleValue() : null);

        // Calculate median using approxQuantile
        try {
            double[] quantiles = dataset.stat().approxQuantile(columnName, new double[]{0.5}, 0.01);
            if (quantiles != null && quantiles.length > 0) {
                builder.median(quantiles[0]);
            }
        } catch (Exception e) {
            logger.warn("Could not calculate median for column: {}", columnName, e);
        }
    }

    /**
     * Profile string column with length statistics.
     */
    private void profileStringColumn(Dataset<Row> dataset, String columnName,
                                    ColumnProfile.Builder builder) {
        Dataset<Row> lengthStats = dataset
            .filter(col(columnName).isNotNull())
            .select(
                min(length(col(columnName))).alias("minLen"),
                max(length(col(columnName))).alias("maxLen"),
                avg(length(col(columnName))).alias("avgLen")
            );

        Row statsRow = lengthStats.first();

        Object minLenObj = statsRow.get(0);
        Object maxLenObj = statsRow.get(1);
        Object avgLenObj = statsRow.get(2);

        builder.minLength(minLenObj != null ? ((Number) minLenObj).intValue() : null);
        builder.maxLength(maxLenObj != null ? ((Number) maxLenObj).intValue() : null);
        builder.avgLength(avgLenObj != null ? ((Number) avgLenObj).doubleValue() : null);
    }

    /**
     * Get top N most frequent values in a column.
     */
    private Map<String, Long> getValueDistribution(Dataset<Row> dataset, String columnName) {
        Map<String, Long> distribution = new HashMap<>();

        try {
            List<Row> topValues = dataset
                .filter(col(columnName).isNotNull())
                .groupBy(columnName)
                .count()
                .orderBy(col("count").desc())
                .limit(topValuesCount)
                .collectAsList();

            for (Row row : topValues) {
                String value = String.valueOf(row.get(0));
                Long count = row.getLong(1);
                distribution.put(value, count);
            }
        } catch (Exception e) {
            logger.warn("Could not calculate value distribution for column: {}", columnName, e);
        }

        return distribution;
    }

    private boolean isNumericType(String dataType) {
        return dataType.contains("int") || dataType.contains("long") ||
               dataType.contains("double") || dataType.contains("float") ||
               dataType.contains("decimal");
    }

    private boolean isStringType(String dataType) {
        return dataType.contains("string");
    }
}
