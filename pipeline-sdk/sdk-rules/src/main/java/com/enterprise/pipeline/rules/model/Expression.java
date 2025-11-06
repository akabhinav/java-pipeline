package com.enterprise.pipeline.rules.model;

import java.util.Map;

/**
 * An expression for transformation rules.
 *
 * Example:
 * Formula: "P * r * (1 + r)^n / ((1 + r)^n - 1)"
 * Variables: {"P": "loan_amount", "r": "monthly_rate", "n": "tenure_months"}
 * Result: "loan_amount * monthly_rate * (1 + monthly_rate)^tenure_months / ..."
 */
public record Expression(
        String formula,
        Map<String, String> variables
) {
    /**
     * Create a simple expression without variable substitution.
     */
    public Expression(String formula) {
        this(formula, null);
    }

    /**
     * Convert to Spark SQL expression by substituting variables.
     */
    public String toSparkExpression() {
        if (variables == null || variables.isEmpty()) {
            return formula;
        }

        String expr = formula;
        for (Map.Entry<String, String> entry : variables.entrySet()) {
            // Replace variable placeholder with actual column name
            expr = expr.replaceAll("\\b" + entry.getKey() + "\\b", entry.getValue());
        }

        return expr;
    }
}
