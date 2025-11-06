package com.enterprise.pipeline.rules.model;

/**
 * An action to perform when a rule condition is met.
 *
 * Actions can either:
 * - Set a fixed value: {field: "status", value: "APPROVED"}
 * - Use an expression: {field: "total", expression: "amount * quantity"}
 */
public record Action(
        String field,
        Object value,
        String expression
) {
    /**
     * Create an action with a fixed value.
     */
    public Action(String field, Object value) {
        this(field, value, null);
    }

    /**
     * Create an action with an expression.
     */
    public static Action withExpression(String field, String expression) {
        return new Action(field, null, expression);
    }

    /**
     * Check if this action uses an expression.
     */
    public boolean hasExpression() {
        return expression != null && !expression.isEmpty();
    }

    /**
     * Check if this action uses a fixed value.
     */
    public boolean hasValue() {
        return value != null;
    }

    /**
     * Get the value expression (either fixed value or expression).
     */
    public String getValueExpression() {
        if (hasExpression()) {
            return expression;
        }
        if (hasValue()) {
            return formatValue(value);
        }
        return "NULL";
    }

    private String formatValue(Object val) {
        if (val == null) return "NULL";
        if (val instanceof String) return "'" + val.toString().replace("'", "''") + "'";
        if (val instanceof Number || val instanceof Boolean) return val.toString();
        return "'" + val.toString() + "'";
    }
}
