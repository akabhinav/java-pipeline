package com.enterprise.pipeline.quality.rules;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

/**
 * Rule that validates numeric values fall within a specified range.
 */
public class RangeRule implements QualityRule {
    private final String columnName;
    private final double minValue;
    private final double maxValue;
    private final boolean includeMin;
    private final boolean includeMax;
    private final RuleSeverity severity;

    public RangeRule(String columnName, double minValue, double maxValue) {
        this(columnName, minValue, maxValue, true, true, RuleSeverity.ERROR);
    }

    public RangeRule(String columnName, double minValue, double maxValue,
                    boolean includeMin, boolean includeMax) {
        this(columnName, minValue, maxValue, includeMin, includeMax, RuleSeverity.ERROR);
    }

    public RangeRule(String columnName, double minValue, double maxValue,
                    boolean includeMin, boolean includeMax, RuleSeverity severity) {
        this.columnName = columnName;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.includeMin = includeMin;
        this.includeMax = includeMax;
        this.severity = severity;
    }

    @Override
    public String getName() {
        String minOp = includeMin ? ">=" : ">";
        String maxOp = includeMax ? "<=" : "<";
        return "Range(" + columnName + " " + minOp + " " + minValue +
               " AND " + maxOp + " " + maxValue + ")";
    }

    @Override
    public String getDescription() {
        String minOp = includeMin ? ">=" : ">";
        String maxOp = includeMax ? "<=" : "<";
        return "Column '" + columnName + "' should be " + minOp + " " + minValue +
               " and " + maxOp + " " + maxValue;
    }

    @Override
    public Dataset<Row> validate(Dataset<Row> dataset) {
        // Return rows that violate the range
        return dataset.filter(
            col(columnName).isNotNull().and(
                col(columnName).lt(minValue).or(col(columnName).gt(maxValue))
                .or(includeMin ? col(columnName).lt(minValue) : col(columnName).leq(minValue))
                .or(includeMax ? col(columnName).gt(maxValue) : col(columnName).geq(maxValue))
            )
        );
    }

    @Override
    public RuleSeverity getSeverity() {
        return severity;
    }
}
