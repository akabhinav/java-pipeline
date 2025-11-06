package com.enterprise.pipeline.rules.model;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Comparison and logical operators for rule conditions.
 */
public enum Operator {
    // Comparison operators
    EQUALS,
    NOT_EQUALS,
    GREATER_THAN,
    GREATER_THAN_OR_EQUAL,
    LESS_THAN,
    LESS_THAN_OR_EQUAL,
    BETWEEN,

    // Set operations
    IN,
    NOT_IN,

    // String operations
    CONTAINS,
    STARTS_WITH,
    ENDS_WITH,
    MATCHES_REGEX,

    // Null/Empty checks
    IS_NULL,
    IS_NOT_NULL,
    IS_EMPTY,
    IS_NOT_EMPTY;

    /**
     * Convert operator to Spark SQL expression.
     */
    public String toSparkExpression(String field, Object value, Object value2) {
        return switch (this) {
            case EQUALS -> field + " = " + formatValue(value);
            case NOT_EQUALS -> field + " != " + formatValue(value);
            case GREATER_THAN -> field + " > " + formatValue(value);
            case GREATER_THAN_OR_EQUAL -> field + " >= " + formatValue(value);
            case LESS_THAN -> field + " < " + formatValue(value);
            case LESS_THAN_OR_EQUAL -> field + " <= " + formatValue(value);
            case BETWEEN -> field + " BETWEEN " + formatValue(value) + " AND " + formatValue(value2);
            case IN -> field + " IN (" + formatListValue(value) + ")";
            case NOT_IN -> field + " NOT IN (" + formatListValue(value) + ")";
            case CONTAINS -> field + " LIKE '%" + value + "%'";
            case STARTS_WITH -> field + " LIKE '" + value + "%'";
            case ENDS_WITH -> field + " LIKE '%" + value + "'";
            case MATCHES_REGEX -> field + " RLIKE '" + value + "'";
            case IS_NULL -> field + " IS NULL";
            case IS_NOT_NULL -> field + " IS NOT NULL";
            case IS_EMPTY -> "LENGTH(COALESCE(" + field + ", '')) = 0";
            case IS_NOT_EMPTY -> "LENGTH(COALESCE(" + field + ", '')) > 0";
        };
    }

    @SuppressWarnings("unchecked")
    private static String formatListValue(Object value) {
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            return list.stream()
                    .map(Operator::formatValue)
                    .collect(Collectors.joining(", "));
        }
        return formatValue(value);
    }

    private static String formatValue(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof String) {
            return "'" + value.toString().replace("'", "''") + "'";
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        return "'" + value.toString() + "'";
    }
}
