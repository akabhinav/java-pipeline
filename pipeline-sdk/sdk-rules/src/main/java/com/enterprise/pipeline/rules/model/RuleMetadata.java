package com.enterprise.pipeline.rules.model;

import java.time.LocalDateTime;

/**
 * Metadata about rule creation and modification.
 */
public record RuleMetadata(
        String createdBy,
        LocalDateTime createdAt,
        String modifiedBy,
        LocalDateTime modifiedAt,
        int version
) {
    /**
     * Create initial metadata for a new rule.
     */
    public static RuleMetadata create(String createdBy) {
        return new RuleMetadata(
                createdBy,
                LocalDateTime.now(),
                null,
                null,
                1
        );
    }

    /**
     * Create updated metadata when rule is modified.
     */
    public RuleMetadata withUpdate(String modifiedBy) {
        return new RuleMetadata(
                this.createdBy,
                this.createdAt,
                modifiedBy,
                LocalDateTime.now(),
                this.version + 1
        );
    }
}
