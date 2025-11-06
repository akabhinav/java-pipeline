package com.enterprise.pipeline.rules.model;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A group of conditions connected by AND/OR/NOT operators.
 *
 * Supports nested groups for complex logic.
 *
 * Example:
 * AND(
 *   credit_score >= 650,
 *   OR(
 *     income > 50000,
 *     assets > 100000
 *   )
 * )
 */
public record ConditionGroup(
        String type,  // "AND", "OR", "NOT"
        List<Object> rules  // List of Condition or ConditionGroup
) {
    /**
     * Create an AND group.
     */
    public static ConditionGroup and(Object... rules) {
        return new ConditionGroup("AND", List.of(rules));
    }

    /**
     * Create an OR group.
     */
    public static ConditionGroup or(Object... rules) {
        return new ConditionGroup("OR", List.of(rules));
    }

    /**
     * Create a NOT group.
     */
    public static ConditionGroup not(Object rule) {
        return new ConditionGroup("NOT", List.of(rule));
    }

    /**
     * Convert to Spark SQL expression.
     */
    public String toSparkExpression() {
        if (rules == null || rules.isEmpty()) {
            return "TRUE";
        }

        List<String> expressions = new ArrayList<>();
        for (Object rule : rules) {
            if (rule instanceof Condition condition) {
                expressions.add("(" + condition.toSparkExpression() + ")");
            } else if (rule instanceof ConditionGroup group) {
                expressions.add("(" + group.toSparkExpression() + ")");
            }
        }

        String combined = String.join(" " + type + " ", expressions);

        if ("NOT".equalsIgnoreCase(type)) {
            return "NOT (" + combined + ")";
        }

        return combined;
    }

    /**
     * Add a condition to this group (returns new group).
     */
    public ConditionGroup add(Object rule) {
        List<Object> newRules = new ArrayList<>(this.rules);
        newRules.add(rule);
        return new ConditionGroup(this.type, newRules);
    }
}
