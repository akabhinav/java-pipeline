package com.enterprise.pipeline.rules.model;

/**
 * A single condition in a rule.
 *
 * Examples:
 * - Simple: credit_score >= 650
 * - Between: amount BETWEEN 1000 AND 10000
 * - In: status IN ('APPROVED', 'PENDING')
 * - Field comparison: amount > balance
 */
public record Condition(
        String field,
        Operator operator,
        Object value,
        Object value2,  // For BETWEEN operator
        String compareField  // For field-to-field comparison
) {
    /**
     * Create a simple condition with field and value.
     */
    public Condition(String field, Operator operator, Object value) {
        this(field, operator, value, null, null);
    }

    /**
     * Create a BETWEEN condition.
     */
    public static Condition between(String field, Object min, Object max) {
        return new Condition(field, Operator.BETWEEN, min, max, null);
    }

    /**
     * Create a field comparison condition.
     */
    public static Condition compareFields(String field, Operator operator, String compareField) {
        return new Condition(field, operator, null, null, compareField);
    }

    /**
     * Convert to Spark SQL expression.
     */
    public String toSparkExpression() {
        if (compareField != null) {
            // Field-to-field comparison
            return field + " " + operatorSymbol() + " " + compareField;
        }
        return operator.toSparkExpression(field, value, value2);
    }

    private String operatorSymbol() {
        return switch (operator) {
            case EQUALS -> "=";
            case NOT_EQUALS -> "!=";
            case GREATER_THAN -> ">";
            case GREATER_THAN_OR_EQUAL -> ">=";
            case LESS_THAN -> "<";
            case LESS_THAN_OR_EQUAL -> "<=";
            default -> throw new IllegalStateException(
                    "Operator " + operator + " not supported for field comparison");
        };
    }
}
