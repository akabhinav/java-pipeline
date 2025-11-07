package com.enterprise.pipeline.quality.rules;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

/**
 * Rule that validates string values match a regular expression pattern.
 */
public class RegexRule implements QualityRule {
    private final String columnName;
    private final String pattern;
    private final RuleSeverity severity;

    public RegexRule(String columnName, String pattern) {
        this(columnName, pattern, RuleSeverity.ERROR);
    }

    public RegexRule(String columnName, String pattern, RuleSeverity severity) {
        this.columnName = columnName;
        this.pattern = pattern;
        this.severity = severity;
    }

    @Override
    public String getName() {
        return "Regex(" + columnName + ")";
    }

    @Override
    public String getDescription() {
        return "Column '" + columnName + "' should match pattern: " + pattern;
    }

    @Override
    public Dataset<Row> validate(Dataset<Row> dataset) {
        // Return rows where value doesn't match pattern
        return dataset.filter(
            col(columnName).isNotNull().and(
                col(columnName).rlike(pattern).not()
            )
        );
    }

    @Override
    public RuleSeverity getSeverity() {
        return severity;
    }

    // Common pattern factories
    public static RegexRule email(String columnName) {
        return new RegexRule(columnName,
            "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$");
    }

    public static RegexRule phone(String columnName) {
        return new RegexRule(columnName,
            "^\\+?[1-9]\\d{1,14}$"); // E.164 format
    }

    public static RegexRule zipCode(String columnName) {
        return new RegexRule(columnName,
            "^\\d{5}(-\\d{4})?$");
    }

    public static RegexRule ssn(String columnName) {
        return new RegexRule(columnName,
            "^\\d{3}-\\d{2}-\\d{4}$");
    }
}
