package com.enterprise.pipeline.quality.metrics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;

import static org.apache.spark.sql.functions.col;

/**
 * Accuracy metric - measures how many values match a validation condition.
 * Score = (values matching condition / total non-null values) * 100
 */
public class AccuracyMetric implements QualityMetric {
    private final String columnName;
    private final Column validationCondition;
    private final String conditionDescription;

    /**
     * Create an accuracy metric with a validation condition.
     *
     * @param columnName Column to validate
     * @param validationCondition Spark SQL condition that valid values should satisfy
     * @param conditionDescription Human-readable description of the condition
     */
    public AccuracyMetric(String columnName, Column validationCondition,
                         String conditionDescription) {
        this.columnName = columnName;
        this.validationCondition = validationCondition;
        this.conditionDescription = conditionDescription;
    }

    /**
     * Create an accuracy metric with a SQL expression.
     */
    public static AccuracyMetric withExpression(String columnName, String sqlExpression) {
        return new AccuracyMetric(
            columnName,
            org.apache.spark.sql.functions.expr(sqlExpression),
            sqlExpression
        );
    }

    /**
     * Create a range validation metric.
     */
    public static AccuracyMetric range(String columnName, double min, double max) {
        return new AccuracyMetric(
            columnName,
            col(columnName).between(min, max),
            columnName + " between " + min + " and " + max
        );
    }

    /**
     * Create a regex validation metric.
     */
    public static AccuracyMetric regex(String columnName, String pattern) {
        return new AccuracyMetric(
            columnName,
            col(columnName).rlike(pattern),
            columnName + " matches '" + pattern + "'"
        );
    }

    @Override
    public String getName() {
        return "Accuracy(" + columnName + ")";
    }

    @Override
    public double calculate(Dataset<Row> dataset) {
        // Count non-null values
        long nonNullCount = dataset.filter(col(columnName).isNotNull()).count();

        if (nonNullCount == 0) {
            return 100.0; // No data to validate
        }

        // Count values that pass validation
        long validCount = dataset
            .filter(col(columnName).isNotNull())
            .filter(validationCondition)
            .count();

        return (validCount * 100.0) / nonNullCount;
    }

    @Override
    public String getDescription() {
        return "Percentage of values matching condition: " + conditionDescription;
    }
}
