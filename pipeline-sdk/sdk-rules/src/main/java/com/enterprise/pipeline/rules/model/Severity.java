package com.enterprise.pipeline.rules.model;

/**
 * Severity levels for validation rules.
 */
public enum Severity {
    /**
     * Informational only - no action required.
     */
    INFO,

    /**
     * Warning - should be reviewed but not critical.
     */
    WARNING,

    /**
     * Error - should be addressed.
     */
    ERROR,

    /**
     * Critical - requires immediate attention.
     */
    CRITICAL
}
