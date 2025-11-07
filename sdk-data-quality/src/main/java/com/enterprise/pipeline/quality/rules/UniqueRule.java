package com.enterprise.pipeline.quality.rules;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

/**
 * Rule that validates uniqueness of values across one or more columns.
 */
public class UniqueRule implements QualityRule {
    private final String[] columns;
    private final RuleSeverity severity;

    public UniqueRule(String... columns) {
        this(RuleSeverity.ERROR, columns);
    }

    public UniqueRule(RuleSeverity severity, String... columns) {
        this.columns = columns;
        this.severity = severity;
    }

    @Override
    public String getName() {
        return "Unique(" + String.join(", ", columns) + ")";
    }

    @Override
    public String getDescription() {
        return "Columns [" + String.join(", ", columns) + "] should have unique values";
    }

    @Override
    public Dataset<Row> validate(Dataset<Row> dataset) {
        // Find duplicates by grouping and counting
        Dataset<Row> grouped = dataset
            .groupBy(columns)
            .agg(count("*").alias("_count"));

        // Get the duplicate keys
        Dataset<Row> duplicateKeys = grouped.filter(col("_count").gt(1))
            .drop("_count");

        // Return original rows that are duplicates
        if (duplicateKeys.count() == 0) {
            return dataset.limit(0); // Return empty dataset
        }

        // Join back to original dataset to get all duplicate rows
        return dataset.join(duplicateKeys, Arrays.asList(columns), "inner");
    }

    @Override
    public RuleSeverity getSeverity() {
        return severity;
    }
}
