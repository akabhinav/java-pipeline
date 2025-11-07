package com.enterprise.pipeline.quality.rules;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * Engine for executing multiple quality rules and aggregating results.
 */
public class RuleEngine implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(RuleEngine.class);

    private final List<QualityRule> rules;
    private final boolean failFast;

    private RuleEngine(Builder builder) {
        this.rules = builder.rules;
        this.failFast = builder.failFast;
    }

    /**
     * Execute all rules and return results.
     */
    public RuleExecutionResult execute(Dataset<Row> dataset) {
        logger.info("Executing {} quality rules", rules.size());

        Map<String, RuleViolation> violations = new LinkedHashMap<>();
        int passedCount = 0;
        int failedCount = 0;

        for (QualityRule rule : rules) {
            logger.debug("Executing rule: {}", rule.getName());

            try {
                Dataset<Row> violatingRows = rule.validate(dataset);
                long violationCount = violatingRows.count();

                if (violationCount > 0) {
                    logger.warn("Rule '{}' failed with {} violations",
                        rule.getName(), violationCount);

                    violations.put(rule.getName(),
                        new RuleViolation(rule, violationCount, violatingRows));
                    failedCount++;

                    if (failFast && rule.getSeverity() == QualityRule.RuleSeverity.CRITICAL) {
                        logger.error("Critical rule failed, stopping execution");
                        break;
                    }
                } else {
                    logger.debug("Rule '{}' passed", rule.getName());
                    passedCount++;
                }
            } catch (Exception e) {
                logger.error("Error executing rule: " + rule.getName(), e);
                violations.put(rule.getName(),
                    new RuleViolation(rule, -1, null, e));
                failedCount++;
            }
        }

        logger.info("Rule execution complete: {} passed, {} failed",
            passedCount, failedCount);

        return new RuleExecutionResult(passedCount, failedCount, violations);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final List<QualityRule> rules = new ArrayList<>();
        private boolean failFast = false;

        public Builder add(QualityRule rule) {
            rules.add(rule);
            return this;
        }

        public Builder addNotNull(String columnName) {
            return add(new NotNullRule(columnName));
        }

        public Builder addRange(String columnName, double min, double max) {
            return add(new RangeRule(columnName, min, max));
        }

        public Builder addRegex(String columnName, String pattern) {
            return add(new RegexRule(columnName, pattern));
        }

        public Builder addUnique(String... columns) {
            return add(new UniqueRule(columns));
        }

        public Builder failFast(boolean failFast) {
            this.failFast = failFast;
            return this;
        }

        public RuleEngine build() {
            return new RuleEngine(this);
        }
    }

    /**
     * Represents a rule violation.
     */
    public static class RuleViolation implements Serializable {
        private final QualityRule rule;
        private final long violationCount;
        private final Dataset<Row> violatingRows;
        private final Exception error;

        public RuleViolation(QualityRule rule, long violationCount,
                           Dataset<Row> violatingRows) {
            this(rule, violationCount, violatingRows, null);
        }

        public RuleViolation(QualityRule rule, long violationCount,
                           Dataset<Row> violatingRows, Exception error) {
            this.rule = rule;
            this.violationCount = violationCount;
            this.violatingRows = violatingRows;
            this.error = error;
        }

        public QualityRule getRule() { return rule; }
        public long getViolationCount() { return violationCount; }
        public Dataset<Row> getViolatingRows() { return violatingRows; }
        public Exception getError() { return error; }
        public boolean hasError() { return error != null; }
    }

    /**
     * Result of rule execution.
     */
    public static class RuleExecutionResult implements Serializable {
        private final int passedCount;
        private final int failedCount;
        private final Map<String, RuleViolation> violations;

        public RuleExecutionResult(int passedCount, int failedCount,
                                  Map<String, RuleViolation> violations) {
            this.passedCount = passedCount;
            this.failedCount = failedCount;
            this.violations = violations;
        }

        public int getPassedCount() { return passedCount; }
        public int getFailedCount() { return failedCount; }
        public int getTotalCount() { return passedCount + failedCount; }
        public Map<String, RuleViolation> getViolations() { return violations; }

        public boolean allPassed() {
            return failedCount == 0;
        }

        public long getTotalViolations() {
            return violations.values().stream()
                .mapToLong(RuleViolation::getViolationCount)
                .sum();
        }

        public String getSummary() {
            StringBuilder sb = new StringBuilder();
            sb.append("=".repeat(80)).append("\n");
            sb.append("QUALITY RULES EXECUTION SUMMARY\n");
            sb.append("=".repeat(80)).append("\n");
            sb.append("Total Rules: ").append(getTotalCount()).append("\n");
            sb.append("Passed: ").append(passedCount).append("\n");
            sb.append("Failed: ").append(failedCount).append("\n");
            sb.append("Total Violations: ").append(getTotalViolations()).append("\n");
            sb.append("=".repeat(80)).append("\n\n");

            if (!violations.isEmpty()) {
                sb.append("VIOLATIONS:\n");
                sb.append("-".repeat(80)).append("\n");

                for (Map.Entry<String, RuleViolation> entry : violations.entrySet()) {
                    RuleViolation violation = entry.getValue();
                    sb.append("Rule: ").append(entry.getKey()).append("\n");
                    sb.append("  Description: ").append(violation.getRule().getDescription()).append("\n");
                    sb.append("  Severity: ").append(violation.getRule().getSeverity()).append("\n");
                    sb.append("  Violations: ").append(violation.getViolationCount()).append("\n");

                    if (violation.hasError()) {
                        sb.append("  Error: ").append(violation.getError().getMessage()).append("\n");
                    }

                    sb.append("\n");
                }
                sb.append("=".repeat(80)).append("\n");
            }

            return sb.toString();
        }

        @Override
        public String toString() {
            return getSummary();
        }
    }
}
