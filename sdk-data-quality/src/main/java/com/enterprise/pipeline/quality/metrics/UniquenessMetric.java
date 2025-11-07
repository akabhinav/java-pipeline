package com.enterprise.pipeline.quality.metrics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.List;

/**
 * Uniqueness metric - measures the percentage of unique values.
 * Score = (distinct values / total values) * 100
 */
public class UniquenessMetric implements QualityMetric {
    private final List<String> columns;

    public UniquenessMetric(String... columns) {
        this.columns = Arrays.asList(columns);
    }

    @Override
    public String getName() {
        return "Uniqueness(" + String.join(", ", columns) + ")";
    }

    @Override
    public double calculate(Dataset<Row> dataset) {
        long totalCount = dataset.count();
        if (totalCount == 0) {
            return 100.0;
        }

        String[] colArray = columns.toArray(new String[0]);
        long distinctCount = dataset.select(colArray).distinct().count();

        return (distinctCount * 100.0) / totalCount;
    }

    @Override
    public String getDescription() {
        return "Percentage of unique values in columns: " + String.join(", ", columns);
    }
}
