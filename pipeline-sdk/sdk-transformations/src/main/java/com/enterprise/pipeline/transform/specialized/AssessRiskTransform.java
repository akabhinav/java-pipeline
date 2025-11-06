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
 * Assess customer or transaction risk based on multiple factors.
 *
 * Config:
 * - creditScoreColumn: Column containing credit score (optional)
 * - incomeColumn: Column containing annual income (optional)
 * - debtToIncomeColumn: Column containing debt-to-income ratio (optional)
 * - employmentYearsColumn: Column containing years of employment (optional)
 * - latePaymentsColumn: Column containing number of late payments (optional)
 * - bankruptciesColumn: Column containing number of bankruptcies (optional)
 * - riskScoreColumn: Output column for risk score (default: "risk_score")
 * - riskCategoryColumn: Output column for risk category (default: "risk_category")
 *
 * Risk Score: 0-100 (0 = lowest risk, 100 = highest risk)
 * Risk Categories: VERY_LOW, LOW, MODERATE, HIGH, VERY_HIGH
 *
 * Example:
 * {
 *   "type": "assessRisk",
 *   "config": {
 *     "creditScoreColumn": "credit_score",
 *     "incomeColumn": "annual_income",
 *     "debtToIncomeColumn": "debt_to_income_ratio",
 *     "latePaymentsColumn": "late_payments",
 *     "riskScoreColumn": "customer_risk_score",
 *     "riskCategoryColumn": "risk_level"
 *   }
 * }
 *
 * @author Enterprise Data Pipeline Team
 */
public class AssessRiskTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(AssessRiskTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "assessRisk";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        // At least one risk factor must be provided
        if (!config.containsKey("creditScoreColumn") &&
            !config.containsKey("incomeColumn") &&
            !config.containsKey("debtToIncomeColumn") &&
            !config.containsKey("employmentYearsColumn") &&
            !config.containsKey("latePaymentsColumn") &&
            !config.containsKey("bankruptciesColumn")) {
            throw new ValidationException("At least one risk factor column is required");
        }
    }

    @Override
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            String riskScoreColumn = config.containsKey("riskScoreColumn")
                    ? (String) config.get("riskScoreColumn")
                    : "risk_score";
            String riskCategoryColumn = config.containsKey("riskCategoryColumn")
                    ? (String) config.get("riskCategoryColumn")
                    : "risk_category";

            logger.debug("Calculating risk assessment");

            Dataset<Row> result = input;

            // Build risk score calculation based on available factors
            org.apache.spark.sql.Column riskScore = functions.lit(0);

            // Factor 1: Credit Score (0-40 points)
            // Lower credit score = higher risk
            if (config.containsKey("creditScoreColumn")) {
                String creditScoreColumn = (String) config.get("creditScoreColumn");
                riskScore = riskScore.plus(
                        functions.when(functions.col(creditScoreColumn).$less(580), 40)
                                .when(functions.col(creditScoreColumn).$less(670), 30)
                                .when(functions.col(creditScoreColumn).$less(740), 20)
                                .when(functions.col(creditScoreColumn).$less(800), 10)
                                .otherwise(5)
                );
                logger.debug("Including credit score in risk assessment");
            }

            // Factor 2: Debt-to-Income Ratio (0-25 points)
            // Higher DTI = higher risk
            if (config.containsKey("debtToIncomeColumn")) {
                String debtToIncomeColumn = (String) config.get("debtToIncomeColumn");
                riskScore = riskScore.plus(
                        functions.when(functions.col(debtToIncomeColumn).$greater(0.5), 25)
                                .when(functions.col(debtToIncomeColumn).$greater(0.43), 20)
                                .when(functions.col(debtToIncomeColumn).$greater(0.36), 15)
                                .when(functions.col(debtToIncomeColumn).$greater(0.28), 10)
                                .otherwise(5)
                );
                logger.debug("Including debt-to-income ratio in risk assessment");
            }

            // Factor 3: Late Payments (0-20 points)
            // More late payments = higher risk
            if (config.containsKey("latePaymentsColumn")) {
                String latePaymentsColumn = (String) config.get("latePaymentsColumn");
                riskScore = riskScore.plus(
                        functions.when(functions.col(latePaymentsColumn).$greater$eq(5), 20)
                                .when(functions.col(latePaymentsColumn).$greater$eq(3), 15)
                                .when(functions.col(latePaymentsColumn).$greater$eq(1), 10)
                                .otherwise(0)
                );
                logger.debug("Including late payments in risk assessment");
            }

            // Factor 4: Income Level (0-10 points)
            // Lower income = higher risk
            if (config.containsKey("incomeColumn")) {
                String incomeColumn = (String) config.get("incomeColumn");
                riskScore = riskScore.plus(
                        functions.when(functions.col(incomeColumn).$less(25000), 10)
                                .when(functions.col(incomeColumn).$less(40000), 7)
                                .when(functions.col(incomeColumn).$less(60000), 4)
                                .otherwise(0)
                );
                logger.debug("Including income in risk assessment");
            }

            // Factor 5: Employment Stability (0-10 points)
            // Less employment years = higher risk
            if (config.containsKey("employmentYearsColumn")) {
                String employmentYearsColumn = (String) config.get("employmentYearsColumn");
                riskScore = riskScore.plus(
                        functions.when(functions.col(employmentYearsColumn).$less(1), 10)
                                .when(functions.col(employmentYearsColumn).$less(2), 7)
                                .when(functions.col(employmentYearsColumn).$less(5), 4)
                                .otherwise(0)
                );
                logger.debug("Including employment years in risk assessment");
            }

            // Factor 6: Bankruptcies (0-15 points)
            // Any bankruptcy = high risk
            if (config.containsKey("bankruptciesColumn")) {
                String bankruptciesColumn = (String) config.get("bankruptciesColumn");
                riskScore = riskScore.plus(
                        functions.when(functions.col(bankruptciesColumn).$greater$eq(2), 15)
                                .when(functions.col(bankruptciesColumn).$greater$eq(1), 12)
                                .otherwise(0)
                );
                logger.debug("Including bankruptcies in risk assessment");
            }

            // Add risk score column
            result = result.withColumn(riskScoreColumn, riskScore);

            // Add risk category based on score
            // 0-20: VERY_LOW
            // 21-40: LOW
            // 41-60: MODERATE
            // 61-80: HIGH
            // 81-100: VERY_HIGH
            result = result.withColumn(riskCategoryColumn,
                    functions.when(functions.col(riskScoreColumn).$less$eq(20), "VERY_LOW")
                            .when(functions.col(riskScoreColumn).$less$eq(40), "LOW")
                            .when(functions.col(riskScoreColumn).$less$eq(60), "MODERATE")
                            .when(functions.col(riskScoreColumn).$less$eq(80), "HIGH")
                            .otherwise("VERY_HIGH")
            );

            return result;

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to assess risk: " + e.getMessage(), e);
        }
    }
}
