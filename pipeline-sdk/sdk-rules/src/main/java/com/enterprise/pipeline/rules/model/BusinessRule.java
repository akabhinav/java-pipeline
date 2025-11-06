package com.enterprise.pipeline.rules.model;

import java.util.List;

/**
 * Business rule - applies conditional logic with actions.
 *
 * Example:
 * {
 *   "ruleType": "BUSINESS",
 *   "ruleName": "loan_approval",
 *   "conditions": {
 *     "type": "AND",
 *     "rules": [
 *       {"field": "credit_score", "operator": ">=", "value": 650},
 *       {"field": "dti", "operator": "<", "value": 0.43}
 *     ]
 *   },
 *   "actions": [
 *     {"field": "status", "value": "APPROVED"}
 *   ],
 *   "elseActions": [
 *     {"field": "status", "value": "REJECTED"}
 *   ]
 * }
 */
public record BusinessRule(
        String ruleId,
        String ruleName,
        String description,
        boolean enabled,
        int priority,
        List<String> tags,
        ConditionGroup conditions,
        List<Action> actions,
        List<Action> elseActions,
        RuleMetadata metadata
) implements Rule {

    @Override
    public RuleType ruleType() {
        return RuleType.BUSINESS;
    }

    @Override
    public Severity severity() {
        return null;  // Not applicable for business rules
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
        private List<Action> actions = List.of();
        private List<Action> elseActions = List.of();
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

        public Builder actions(List<Action> actions) {
            this.actions = actions;
            return this;
        }

        public Builder elseActions(List<Action> elseActions) {
            this.elseActions = elseActions;
            return this;
        }

        public BusinessRule build() {
            if (metadata == null) {
                metadata = RuleMetadata.create("system");
            }
            return new BusinessRule(
                    ruleId,
                    ruleName,
                    description,
                    enabled,
                    priority,
                    tags,
                    conditions,
                    actions,
                    elseActions,
                    metadata
            );
        }
    }
}
