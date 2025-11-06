package com.enterprise.pipeline.rules.executor;

import com.enterprise.pipeline.rules.model.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main rule executor that converts rule definitions to Spark operations.
 *
 * Supports all rule types:
 * - ValidationRule: Adds boolean column indicating pass/fail
 * - TransformationRule: Applies expression to create new column
 * - BusinessRule: Applies conditional logic with actions
 * - FilterRule: Filters rows based on conditions
 *
 * @author Enterprise Data Pipeline Team
 */
public class RuleExecutor {

    private static final Logger logger = LoggerFactory.getLogger(RuleExecutor.class);

    /**
     * Execute a rule on a dataset.
     *
     * @param input The input dataset
     * @param rule The rule to execute
     * @return The dataset with rule applied
     */
    public Dataset<Row> execute(Dataset<Row> input, Rule rule) {
        if (!rule.enabled()) {
            logger.info("Rule '{}' is disabled, skipping execution", rule.ruleName());
            return input;
        }

        logger.info("Executing {} rule: {}", rule.ruleType(), rule.ruleName());

        return switch (rule.ruleType()) {
            case VALIDATION -> executeValidation(input, (ValidationRule) rule);
            case TRANSFORMATION -> executeTransformation(input, (TransformationRule) rule);
            case BUSINESS -> executeBusiness(input, (BusinessRule) rule);
            case FILTER -> executeFilter(input, (FilterRule) rule);
        };
    }

    /**
     * Execute validation rule.
     * Adds a boolean column indicating whether each row passes validation.
     */
    private Dataset<Row> executeValidation(Dataset<Row> input, ValidationRule rule) {
        String conditionExpr = rule.conditions().toSparkExpression();
        String outputColumn = rule.outputColumn();

        logger.debug("Validation expression: {}", conditionExpr);

        try {
            return input.withColumn(outputColumn, functions.expr(conditionExpr));
        } catch (Exception e) {
            logger.error("Failed to execute validation rule '{}': {}", rule.ruleName(), e.getMessage());
            throw new RuntimeException("Validation rule execution failed", e);
        }
    }

    /**
     * Execute transformation rule.
     * Applies the expression and adds/updates a column.
     */
    private Dataset<Row> executeTransformation(Dataset<Row> input, TransformationRule rule) {
        String expr = rule.expression().toSparkExpression();
        String outputColumn = rule.outputColumn();

        logger.debug("Transformation expression: {} -> {}", expr, outputColumn);

        try {
            return input.withColumn(outputColumn, functions.expr(expr));
        } catch (Exception e) {
            logger.error("Failed to execute transformation rule '{}': {}", rule.ruleName(), e.getMessage());
            throw new RuntimeException("Transformation rule execution failed", e);
        }
    }

    /**
     * Execute business rule.
     * Applies conditional logic with actions and else actions.
     */
    private Dataset<Row> executeBusiness(Dataset<Row> input, BusinessRule rule) {
        Dataset<Row> result = input;
        String conditionExpr = rule.conditions().toSparkExpression();

        logger.debug("Business rule condition: {}", conditionExpr);

        try {
            // Apply actions for rows that match conditions
            for (Action action : rule.actions()) {
                String actionExpr = buildActionExpression(action, conditionExpr, true, result);
                result = result.withColumn(action.field(), functions.expr(actionExpr));
                logger.debug("Applied action: {} = {}", action.field(), actionExpr);
            }

            // Apply else actions for rows that don't match
            if (rule.elseActions() != null && !rule.elseActions().isEmpty()) {
                for (Action action : rule.elseActions()) {
                    String actionExpr = buildActionExpression(action, conditionExpr, false, result);
                    result = result.withColumn(action.field(), functions.expr(actionExpr));
                    logger.debug("Applied else action: {} = {}", action.field(), actionExpr);
                }
            }

            return result;
        } catch (Exception e) {
            logger.error("Failed to execute business rule '{}': {}", rule.ruleName(), e.getMessage());
            throw new RuntimeException("Business rule execution failed", e);
        }
    }

    /**
     * Execute filter rule.
     * Returns only rows that match the conditions.
     */
    private Dataset<Row> executeFilter(Dataset<Row> input, FilterRule rule) {
        String conditionExpr = rule.conditions().toSparkExpression();

        logger.debug("Filter expression: {}", conditionExpr);

        try {
            return input.filter(conditionExpr);
        } catch (Exception e) {
            logger.error("Failed to execute filter rule '{}': {}", rule.ruleName(), e.getMessage());
            throw new RuntimeException("Filter rule execution failed", e);
        }
    }

    /**
     * Build action expression for business rules.
     *
     * @param action The action to build
     * @param conditionExpr The condition expression
     * @param whenTrue Whether this is for the "when true" or "when false" case
     * @param dataset The dataset (used to check if column exists)
     * @return The Spark SQL expression
     */
    private String buildActionExpression(Action action, String conditionExpr,
                                          boolean whenTrue, Dataset<Row> dataset) {
        String valueExpr = action.getValueExpression();
        boolean columnExists = columnExists(dataset, action.field());

        if (whenTrue) {
            // WHEN condition THEN value ELSE keep original (or NULL if new column)
            if (columnExists) {
                return String.format("CASE WHEN %s THEN %s ELSE %s END",
                        conditionExpr, valueExpr, action.field());
            } else {
                return String.format("CASE WHEN %s THEN %s ELSE NULL END",
                        conditionExpr, valueExpr);
            }
        } else {
            // WHEN NOT condition THEN value ELSE keep original (or NULL if new column)
            if (columnExists) {
                return String.format("CASE WHEN NOT (%s) THEN %s ELSE %s END",
                        conditionExpr, valueExpr, action.field());
            } else {
                return String.format("CASE WHEN NOT (%s) THEN %s ELSE NULL END",
                        conditionExpr, valueExpr);
            }
        }
    }

    /**
     * Check if a column exists in the dataset.
     */
    private boolean columnExists(Dataset<Row> dataset, String columnName) {
        for (String col : dataset.columns()) {
            if (col.equals(columnName)) {
                return true;
            }
        }
        return false;
    }
}
