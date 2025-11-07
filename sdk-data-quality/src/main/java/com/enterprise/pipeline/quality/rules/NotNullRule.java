package com.enterprise.pipeline.quality.rules;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

/**
 * Rule that validates columns should not contain null values.
 */
public class NotNullRule implements QualityRule {
    private final String columnName;
    private final RuleSeverity severity;

    public NotNullRule(String columnName) {
        this(columnName, RuleSeverity.ERROR);
    }

    public NotNullRule(String columnName, RuleSeverity severity) {
        this.columnName = columnName;
        this.severity = severity;
    }

    @Override
    public String getName() {
        return "NotNull(" + columnName + ")";
    }

    @Override
    public String getDescription() {
        return "Column '" + columnName + "' should not contain null values";
    }

    @Override
    public Dataset<Row> validate(Dataset<Row> dataset) {
        return dataset.filter(col(columnName).isNull());
    }

    @Override
    public RuleSeverity getSeverity() {
        return severity;
    }
}
