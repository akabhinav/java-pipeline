package com.enterprise.pipeline.rules.model;

/**
 * Types of rules supported by the rule engine.
 */
public enum RuleType {
    /**
     * Validation rules check data quality and flag violations.
     * Example: Check if loan amount is within valid range.
     */
    VALIDATION,

    /**
     * Transformation rules modify data based on conditions.
     * Example: Calculate EMI from loan amount and interest rate.
     */
    TRANSFORMATION,

    /**
     * Business rules implement complex business logic.
     * Example: Loan approval decision based on credit score and DTI.
     */
    BUSINESS,

    /**
     * Filter rules select rows based on conditions.
     * Example: Select high-risk transactions for review.
     */
    FILTER
}
