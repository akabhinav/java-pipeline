package com.enterprise.pipeline.rules.model;

import java.util.List;

/**
 * Transformation rule - applies expressions to calculate new values.
 *
 * Example:
 * {
 *   "ruleType": "TRANSFORMATION",
 *   "ruleName": "calculate_emi",
 *   "expression": {
 *     "formula": "P * r * (1+r)^n / ((1+r)^n-1)",
 *     "variables": {"P": "loan_amount", "r": "monthly_rate", "n": "tenure"}
 *   },
 *   "outputColumn": "monthly_emi"
 * }
 */
public record TransformationRule(
        String ruleId,
        String ruleName,
        String description,
        boolean enabled,
        int priority,
        List<String> tags,
        ConditionGroup conditions,
        Expression expression,
        String outputColumn,
        String outputType,
        RuleMetadata metadata
) implements Rule {

    @Override
    public RuleType ruleType() {
        return RuleType.TRANSFORMATION;
    }

    @Override
    public Severity severity() {
        return null;  // Not applicable for transformations
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String ruleId;
        private String ruleName;
        private String description;
        private boolean enabled = true;
        private int priority = 0;
        private List<String> tags = List.of();
        private ConditionGroup conditions;
        private Expression expression;
        private String outputColumn;
        private String outputType = "string";
        private RuleMetadata metadata;

        public Builder ruleId(String ruleId) {
            this.ruleId = ruleId;
            return this;
        }

        public Builder ruleName(String ruleName) {
            this.ruleName = ruleName;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder expression(Expression expression) {
            this.expression = expression;
            return this;
        }

        public Builder expression(String formula) {
            this.expression = new Expression(formula);
            return this;
        }

        public Builder outputColumn(String outputColumn) {
            this.outputColumn = outputColumn;
            return this;
        }

        public Builder outputType(String outputType) {
            this.outputType = outputType;
            return this;
        }

        public TransformationRule build() {
            if (metadata == null) {
                metadata = RuleMetadata.create("system");
            }
            return new TransformationRule(
                    ruleId,
                    ruleName,
                    description,
                    enabled,
                    priority,
                    tags,
                    conditions,
                    expression,
                    outputColumn,
                    outputType,
                    metadata
            );
        }
    }
}
