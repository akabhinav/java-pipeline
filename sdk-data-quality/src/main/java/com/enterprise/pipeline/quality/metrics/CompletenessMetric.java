package com.enterprise.pipeline.quality.metrics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;

import static org.apache.spark.sql.functions.col;

/**
 * Completeness metric - measures the percentage of non-null values.
 * Score = (total non-null values / total values) * 100
 */
public class CompletenessMetric implements QualityMetric {
    private final String columnName;

    public CompletenessMetric(String columnName) {
        this.columnName = columnName;
    }

    /**
     * Constructor for dataset-level completeness (all columns).
     */
    public CompletenessMetric() {
        this.columnName = null;
    }

    @Override
    public String getName() {
        return columnName != null ?
            "Completeness(" + columnName + ")" :
            "Completeness(overall)";
    }

    @Override
    public double calculate(Dataset<Row> dataset) {
        if (columnName != null) {
            return calculateForColumn(dataset, columnName);
        } else {
            return calculateForDataset(dataset);
        }
    }

    private double calculateForColumn(Dataset<Row> dataset, String column) {
        long totalCount = dataset.count();
        if (totalCount == 0) {
            return 100.0;
        }

        long nonNullCount = dataset.filter(col(column).isNotNull()).count();
        return (nonNullCount * 100.0) / totalCount;
    }

    private double calculateForDataset(Dataset<Row> dataset) {
        long totalCount = dataset.count();
        if (totalCount == 0) {
            return 100.0;
        }

        StructField[] fields = dataset.schema().fields();
        double totalCompleteness = 0.0;

        for (StructField field : fields) {
            totalCompleteness += calculateForColumn(dataset, field.name());
        }

        return totalCompleteness / fields.length;
    }

    @Override
    public String getDescription() {
        return columnName != null ?
            "Percentage of non-null values in column: " + columnName :
            "Average percentage of non-null values across all columns";
    }
}
