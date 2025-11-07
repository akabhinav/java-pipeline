package com.enterprise.pipeline.quality.rules;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;

/**
 * Interface for data quality validation rules.
 * Rules validate data and return violations.
 */
public interface QualityRule extends Serializable {
    /**
     * Get the rule name.
     */
    String getName();

    /**
     * Get the rule description.
     */
    String getDescription();

    /**
     * Validate the dataset and return violations.
     *
     * @return Dataset containing rows that violate this rule
     */
    Dataset<Row> validate(Dataset<Row> dataset);

    /**
     * Check if the dataset passes this rule (no violations).
     */
    default boolean passes(Dataset<Row> dataset) {
        return validate(dataset).count() == 0;
    }

    /**
     * Get violation count.
     */
    default long getViolationCount(Dataset<Row> dataset) {
        return validate(dataset).count();
    }

    /**
     * Get severity level of this rule.
     */
    default RuleSeverity getSeverity() {
        return RuleSeverity.ERROR;
    }

    /**
     * Rule severity levels.
     */
    enum RuleSeverity {
        INFO,
        WARNING,
        ERROR,
        CRITICAL
    }
}
