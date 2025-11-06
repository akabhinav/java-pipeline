package com.enterprise.pipeline.rules.model;

import java.util.List;

/**
 * Filter rule - selects rows based on conditions.
 *
 * Example:
 * {
 *   "ruleType": "FILTER",
 *   "ruleName": "high_value_transactions",
 *   "conditions": {
 *     "type": "OR",
 *     "rules": [
 *       {"field": "amount", "operator": ">", "value": 10000},
 *       {"field": "country", "operator": "IN", "values": ["NG", "KE"]}
 *     ]
 *   }
 * }
 */
public record FilterRule(
        String ruleId,
        String ruleName,
        String description,
        boolean enabled,
        int priority,
        List<String> tags,
        ConditionGroup conditions,
        RuleMetadata metadata
) implements Rule {

    @Override
    public RuleType ruleType() {
        return RuleType.FILTER;
    }

    @Override
    public Severity severity() {
        return null;  // Not applicable for filter rules
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

        public Builder conditions(ConditionGroup conditions) {
            this.conditions = conditions;
            return this;
        }

        public FilterRule build() {
            if (metadata == null) {
                metadata = RuleMetadata.create("system");
            }
            return new FilterRule(
                    ruleId,
                    ruleName,
                    description,
                    enabled,
                    priority,
                    tags,
                    conditions,
                    metadata
            );
        }
    }
}
