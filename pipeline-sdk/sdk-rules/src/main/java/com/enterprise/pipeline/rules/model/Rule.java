package com.enterprise.pipeline.rules.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * Base interface for all rule types.
 * Rules can be defined via UI and applied to datasets.
 *
 * @author Enterprise Data Pipeline Team
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "ruleType"
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = ValidationRule.class, name = "VALIDATION"),
        @JsonSubTypes.Type(value = TransformationRule.class, name = "TRANSFORMATION"),
        @JsonSubTypes.Type(value = BusinessRule.class, name = "BUSINESS"),
        @JsonSubTypes.Type(value = FilterRule.class, name = "FILTER")
})
public interface Rule {

    /**
     * Unique identifier for the rule.
     */
    String ruleId();

    /**
     * Human-readable name for the rule.
     */
    String ruleName();

    /**
     * Optional description of what the rule does.
     */
    String description();

    /**
     * Type of rule (VALIDATION, TRANSFORMATION, BUSINESS, FILTER).
     */
    RuleType ruleType();

    /**
     * Whether this rule is currently enabled.
     */
    boolean enabled();

    /**
     * Priority for rule execution (higher = earlier).
     */
    int priority();

    /**
     * Tags for categorization and search.
     */
    List<String> tags();

    /**
     * Conditions that determine when/how the rule applies.
     */
    ConditionGroup conditions();

    /**
     * Metadata about rule creation and modification.
     */
    RuleMetadata metadata();

    /**
     * Severity level for validation rules.
     */
    Severity severity();

    /**
     * Validate that the rule definition is correct.
     */
    default void validate() {
        if (ruleName() == null || ruleName().trim().isEmpty()) {
            throw new IllegalArgumentException("Rule name is required");
        }
        if (ruleType() == null) {
            throw new IllegalArgumentException("Rule type is required");
        }
    }
}
