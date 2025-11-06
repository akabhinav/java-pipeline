package com.enterprise.pipeline.transform.specialized;

import com.enterprise.pipeline.api.Transformation;
import com.enterprise.pipeline.api.TransformationContext;
import com.enterprise.pipeline.api.TransformationException;
import com.enterprise.pipeline.api.ValidationException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Calculate simplified credit score based on banking behavior.
 *
 * Config:
 * - balanceColumn: Average account balance column
 * - transactionCountColumn: Monthly transaction count column
 * - latePaymentColumn: Late payment count column (optional)
 * - accountAgeColumn: Account age in months column (optional)
 * - scoreColumn: Output credit score column (default: "credit_score")
 * - ratingColumn: Output rating column (default: "credit_rating")
 *
 * Example:
 * {
 *   "type": "calculateCreditScore",
 *   "config": {
 *     "balanceColumn": "avg_balance",
 *     "transactionCountColumn": "monthly_transactions",
 *     "latePaymentColumn": "late_payments",
 *     "accountAgeColumn": "account_age_months",
 *     "scoreColumn": "credit_score",
 *     "ratingColumn": "credit_rating"
 *   }
 * }
 *
 * Score ranges:
 * - 800-850: Excellent
 * - 740-799: Very Good
 * - 670-739: Good
 * - 580-669: Fair
 * - 300-579: Poor
 *
 * @author Enterprise Data Pipeline Team
 */
public class CalculateCreditScoreTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(CalculateCreditScoreTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "calculateCreditScore";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("balanceColumn")) {
            throw new ValidationException("'balanceColumn' is required");
        }
        if (!config.containsKey("transactionCountColumn")) {
            throw new ValidationException("'transactionCountColumn' is required");
        }
    }

    @Override
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            String balanceColumn = (String) config.get("balanceColumn");
            String transactionCountColumn = (String) config.get("transactionCountColumn");
            String latePaymentColumn = config.containsKey("latePaymentColumn")
                    ? (String) config.get("latePaymentColumn")
                    : null;
            String accountAgeColumn = config.containsKey("accountAgeColumn")
                    ? (String) config.get("accountAgeColumn")
                    : null;
            String scoreColumn = config.containsKey("scoreColumn")
                    ? (String) config.get("scoreColumn")
                    : "credit_score";
            String ratingColumn = config.containsKey("ratingColumn")
                    ? (String) config.get("ratingColumn")
                    : "credit_rating";

            logger.debug("Calculating credit score based on balance and transactions");

            // Base score: 300
            // Balance contribution: up to 300 points
            // Transaction activity: up to 150 points
            // Account age: up to 100 points (if provided)
            // Late payment penalty: -50 points each (if provided)

            Dataset<Row> result = input.withColumn(scoreColumn,
                    // Base score
                    functions.lit(300)
                            // Balance score (0-300 based on balance tiers)
                            .plus(functions.when(
                                    functions.col(balanceColumn).$greater(100000), 300
                            ).when(
                                    functions.col(balanceColumn).$greater(50000), 250
                            ).when(
                                    functions.col(balanceColumn).$greater(20000), 200
                            ).when(
                                    functions.col(balanceColumn).$greater(10000), 150
                            ).when(
                                    functions.col(balanceColumn).$greater(5000), 100
                            ).otherwise(50))
                            // Transaction activity score (0-150)
                            .plus(functions.when(
                                    functions.col(transactionCountColumn).$greater(50), 150
                            ).when(
                                    functions.col(transactionCountColumn).$greater(30), 100
                            ).when(
                                    functions.col(transactionCountColumn).$greater(15), 70
                            ).when(
                                    functions.col(transactionCountColumn).$greater(5), 40
                            ).otherwise(20))
            );

            // Add account age bonus if provided
            if (accountAgeColumn != null) {
                result = result.withColumn(scoreColumn,
                        functions.col(scoreColumn)
                                .plus(functions.when(
                                        functions.col(accountAgeColumn).$greater(60), 100
                                ).when(
                                        functions.col(accountAgeColumn).$greater(36), 70
                                ).when(
                                        functions.col(accountAgeColumn).$greater(12), 40
                                ).otherwise(20))
                );
            }

            // Apply late payment penalty if provided
            if (latePaymentColumn != null) {
                result = result.withColumn(scoreColumn,
                        functions.col(scoreColumn)
                                .minus(functions.col(latePaymentColumn).multiply(50))
                );
            }

            // Cap score between 300 and 850
            result = result.withColumn(scoreColumn,
                    functions.when(
                            functions.col(scoreColumn).$less(300), 300
                    ).when(
                            functions.col(scoreColumn).$greater(850), 850
                    ).otherwise(functions.col(scoreColumn))
            );

            // Add rating
            result = result.withColumn(ratingColumn,
                    functions.when(
                            functions.col(scoreColumn).$greater$eq(800), "Excellent"
                    ).when(
                            functions.col(scoreColumn).$greater$eq(740), "Very Good"
                    ).when(
                            functions.col(scoreColumn).$greater$eq(670), "Good"
                    ).when(
                            functions.col(scoreColumn).$greater$eq(580), "Fair"
                    ).otherwise("Poor")
            );

            return result;

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to calculate credit score: " + e.getMessage(), e);
        }
    }
}
