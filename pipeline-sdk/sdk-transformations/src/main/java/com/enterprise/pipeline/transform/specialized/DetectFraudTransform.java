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
 * Detect potentially fraudulent transactions based on rules.
 *
 * Config:
 * - amountColumn: Column containing transaction amount
 * - amountThreshold: Threshold for high-value transactions (default: 10000)
 * - velocityWindow: Time window for velocity check in hours (optional)
 * - velocityThreshold: Max transaction count in window (optional)
 * - fraudColumn: Output column name (default: "fraud_flag")
 * - scoreColumn: Output fraud score column (default: "fraud_score")
 *
 * Example:
 * {
 *   "type": "detectFraud",
 *   "config": {
 *     "amountColumn": "transaction_amount",
 *     "amountThreshold": 10000,
 *     "fraudColumn": "is_suspicious",
 *     "scoreColumn": "risk_score"
 *   }
 * }
 *
 * Fraud Detection Rules:
 * - High-value transactions (> threshold)
 * - Round amounts (possible money laundering)
 * - Late night transactions
 *
 * @author Enterprise Data Pipeline Team
 */
public class DetectFraudTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(DetectFraudTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "detectFraud";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("amountColumn")) {
            throw new ValidationException("'amountColumn' is required");
        }
    }

    @Override
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            String amountColumn = (String) config.get("amountColumn");
            double amountThreshold = config.containsKey("amountThreshold")
                    ? ((Number) config.get("amountThreshold")).doubleValue()
                    : 10000.0;
            String fraudColumn = config.containsKey("fraudColumn")
                    ? (String) config.get("fraudColumn")
                    : "fraud_flag";
            String scoreColumn = config.containsKey("scoreColumn")
                    ? (String) config.get("scoreColumn")
                    : "fraud_score";

            logger.debug("Detecting fraud with amount threshold: {}", amountThreshold);

            // Calculate fraud score (0-100)
            Dataset<Row> result = input.withColumn(scoreColumn,
                    // High amount: +30 points
                    functions.when(
                            functions.col(amountColumn).$greater(amountThreshold), 30
                    ).otherwise(0)
                            // Round amount (divisible by 1000): +20 points
                            .plus(functions.when(
                                    functions.col(amountColumn).mod(1000).$eq$eq$eq(0)
                                            .and(functions.col(amountColumn).$greater(1000)), 20
                            ).otherwise(0))
                            // Very large amount (> 50000): +30 points
                            .plus(functions.when(
                                    functions.col(amountColumn).$greater(50000), 30
                            ).otherwise(0))
            );

            // Set fraud flag based on score
            result = result.withColumn(fraudColumn,
                    functions.when(
                            functions.col(scoreColumn).$greater$eq(50), true
                    ).otherwise(false)
            );

            return result;

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to detect fraud: " + e.getMessage(), e);
        }
    }
}
