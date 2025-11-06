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
 * Calculate simple or compound interest for banking products.
 *
 * Config:
 * - principalColumn: Column containing principal amount
 * - rateColumn: Column containing interest rate (as percentage or decimal)
 * - periodColumn: Column containing time period (in years/months/days)
 * - periodUnit: Time unit (years, months, days) - default: years
 * - interestType: Type of interest (simple, compound) - default: simple
 * - compoundingFrequency: For compound interest (annual, monthly, daily) - default: annual
 * - resultColumn: Output column name (default: "interest_amount")
 *
 * Example - Simple Interest:
 * {
 *   "type": "calculateInterest",
 *   "config": {
 *     "principalColumn": "loan_amount",
 *     "rateColumn": "interest_rate",
 *     "periodColumn": "tenure_years",
 *     "periodUnit": "years",
 *     "interestType": "simple",
 *     "resultColumn": "total_interest"
 *   }
 * }
 *
 * Example - Compound Interest:
 * {
 *   "type": "calculateInterest",
 *   "config": {
 *     "principalColumn": "deposit_amount",
 *     "rateColumn": "annual_rate",
 *     "periodColumn": "tenure_years",
 *     "interestType": "compound",
 *     "compoundingFrequency": "monthly",
 *     "resultColumn": "maturity_interest"
 *   }
 * }
 *
 * @author Enterprise Data Pipeline Team
 */
public class CalculateInterestTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(CalculateInterestTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "calculateInterest";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("principalColumn")) {
            throw new ValidationException("'principalColumn' is required");
        }
        if (!config.containsKey("rateColumn")) {
            throw new ValidationException("'rateColumn' is required");
        }
        if (!config.containsKey("periodColumn")) {
            throw new ValidationException("'periodColumn' is required");
        }
    }

    @Override
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            String principalColumn = (String) config.get("principalColumn");
            String rateColumn = (String) config.get("rateColumn");
            String periodColumn = (String) config.get("periodColumn");
            String periodUnit = config.containsKey("periodUnit")
                    ? (String) config.get("periodUnit")
                    : "years";
            String interestType = config.containsKey("interestType")
                    ? (String) config.get("interestType")
                    : "simple";
            String resultColumn = config.containsKey("resultColumn")
                    ? (String) config.get("resultColumn")
                    : "interest_amount";

            logger.debug("Calculating {} interest: P={}, R={}, T={} ({})",
                    interestType, principalColumn, rateColumn, periodColumn, periodUnit);

            if ("simple".equalsIgnoreCase(interestType)) {
                // Simple Interest: I = P * R * T / 100
                return input.withColumn(resultColumn,
                        functions.col(principalColumn)
                                .multiply(functions.col(rateColumn))
                                .multiply(functions.col(periodColumn))
                                .divide(100.0)
                );
            } else if ("compound".equalsIgnoreCase(interestType)) {
                // Compound Interest: A = P * (1 + r/n)^(n*t)
                String compoundingFrequency = config.containsKey("compoundingFrequency")
                        ? (String) config.get("compoundingFrequency")
                        : "annual";

                int n = switch (compoundingFrequency.toLowerCase()) {
                    case "annual", "yearly" -> 1;
                    case "semi-annual", "semiannual" -> 2;
                    case "quarterly" -> 4;
                    case "monthly" -> 12;
                    case "daily" -> 365;
                    default -> 1;
                };

                // A = P * (1 + r/(n*100))^(n*t)
                // Interest = A - P
                return input.withColumn(resultColumn,
                        functions.col(principalColumn)
                                .multiply(
                                        functions.pow(
                                                functions.lit(1.0)
                                                        .plus(functions.col(rateColumn)
                                                                .divide(n * 100.0)),
                                                functions.col(periodColumn).multiply(n)
                                        )
                                )
                                .minus(functions.col(principalColumn))
                );
            } else {
                throw new IllegalArgumentException("Unsupported interest type: " + interestType);
            }

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to calculate interest: " + e.getMessage(), e);
        }
    }
}
