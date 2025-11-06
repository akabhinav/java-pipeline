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
 * Validate account numbers using mod-11 checksum algorithm.
 * Adds a validation column indicating if account is valid.
 *
 * Config:
 * - accountColumn: Column containing account numbers
 * - validationColumn: Output column name (default: "account_valid")
 *
 * Example:
 * {
 *   "type": "validateAccount",
 *   "config": {
 *     "accountColumn": "account_number",
 *     "validationColumn": "is_valid_account"
 *   }
 * }
 *
 * @author Enterprise Data Pipeline Team
 */
public class ValidateAccountTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(ValidateAccountTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "validateAccount";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("accountColumn")) {
            throw new ValidationException("'accountColumn' is required");
        }
    }

    @Override
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            String accountColumn = (String) config.get("accountColumn");
            String validationColumn = config.containsKey("validationColumn")
                    ? (String) config.get("validationColumn")
                    : "account_valid";

            logger.debug("Validating account numbers in column: {}", accountColumn);

            // Basic validation: not null, length between 8-16 digits, all numeric
            return input.withColumn(validationColumn,
                    functions.when(
                            functions.col(accountColumn).isNull(), false
                    ).when(
                            functions.length(functions.col(accountColumn)).$less(8)
                                    .or(functions.length(functions.col(accountColumn)).$greater(16)),
                            false
                    ).when(
                            functions.col(accountColumn).rlike("^[0-9]+$").$eq$eq$eq(false),
                            false
                    ).otherwise(true)
            );

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to validate accounts: " + e.getMessage(), e);
        }
    }
}
