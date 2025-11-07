package com.enterprise.pipeline.quality.metrics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;

/**
 * Consistency metric - measures cross-field consistency.
 * Score = (rows satisfying consistency rule / total rows) * 100
 *
 * Example: end_date >= start_date
 */
public class ConsistencyMetric implements QualityMetric {
    private final Column consistencyRule;
    private final String ruleDescription;

    /**
     * Create a consistency metric with a validation rule.
     *
     * @param consistencyRule Spark SQL condition that should be true
     * @param ruleDescription Human-readable description
     */
    public ConsistencyMetric(Column consistencyRule, String ruleDescription) {
        this.consistencyRule = consistencyRule;
        this.ruleDescription = ruleDescription;
    }

    /**
     * Create a consistency metric with a SQL expression.
     */
    public static ConsistencyMetric withExpression(String sqlExpression) {
        return new ConsistencyMetric(
            org.apache.spark.sql.functions.expr(sqlExpression),
            sqlExpression
        );
    }

    @Override
    public String getName() {
        return "Consistency";
    }

    @Override
    public double calculate(Dataset<Row> dataset) {
        long totalCount = dataset.count();
        if (totalCount == 0) {
            return 100.0;
        }

        long consistentCount = dataset.filter(consistencyRule).count();

        return (consistentCount * 100.0) / totalCount;
    }

    @Override
    public String getDescription() {
        return "Percentage of rows satisfying rule: " + ruleDescription;
    }
}
