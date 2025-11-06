package com.enterprise.pipeline.rules.model;

import java.util.List;

/**
 * Validation rule - checks data quality and flags violations.
 *
 * Example:
 * {
 *   "ruleType": "VALIDATION",
 *   "ruleName": "validate_loan_amount",
 *   "conditions": [{
 *     "field": "loan_amount",
 *     "operator": "BETWEEN",
 *     "values": [1000, 1000000]
 *   }],
 *   "severity": "ERROR",
 *   "outputColumn": "loan_amount_valid"
 * }
 */
public record ValidationRule(
        String ruleId,
        String ruleName,
        String description,
        boolean enabled,
        int priority,
        List<String> tags,
        ConditionGroup conditions,
        Severity severity,
        String outputColumn,
        RuleMetadata metadata
) implements Rule {

    @Override
    public RuleType ruleType() {
        return RuleType.VALIDATION;
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
        private Severity severity = Severity.ERROR;
        private String outputColumn;
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

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }

        public Builder tags(List<String> tags) {
            this.tags = tags;
            return this;
        }

        public Builder conditions(ConditionGroup conditions) {
            this.conditions = conditions;
            return this;
        }

        public Builder severity(Severity severity) {
            this.severity = severity;
            return this;
        }

        public Builder outputColumn(String outputColumn) {
            this.outputColumn = outputColumn;
            return this;
        }

        public Builder metadata(RuleMetadata metadata) {
            this.metadata = metadata;
            return this;
        }

        public ValidationRule build() {
            if (metadata == null) {
                metadata = RuleMetadata.create("system");
            }
            if (outputColumn == null) {
                outputColumn = ruleName + "_valid";
            }
            return new ValidationRule(
                    ruleId,
                    ruleName,
                    description,
                    enabled,
                    priority,
                    tags,
                    conditions,
                    severity,
                    outputColumn,
                    metadata
            );
        }
    }
}
