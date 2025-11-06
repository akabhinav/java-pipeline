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
 * Mask credit card numbers for PCI compliance.
 * Shows only last 4 digits, masks rest with asterisks.
 *
 * Config:
 * - column: Column containing credit card numbers
 * - maskChar: Character to use for masking (default: '*')
 *
 * Example:
 * {
 *   "type": "maskCreditCard",
 *   "config": {
 *     "column": "card_number",
 *     "maskChar": "X"
 *   }
 * }
 *
 * Result: "1234567890123456" becomes "************3456"
 *
 * @author Enterprise Data Pipeline Team
 */
public class MaskCreditCardTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(MaskCreditCardTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "maskCreditCard";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("column")) {
            throw new ValidationException("'column' is required");
        }
    }

    @Override
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            String column = (String) config.get("column");
            String maskChar = config.containsKey("maskChar")
                    ? (String) config.get("maskChar")
                    : "*";

            logger.debug("Masking credit card numbers in column: {}", column);

            // Mask all but last 4 digits
            return input.withColumn(column,
                    functions.when(
                            functions.col(column).isNull(),
                            functions.lit(null)
                    ).otherwise(
                            functions.concat(
                                    functions.expr(String.format(
                                            "repeat('%s', length(%s) - 4)",
                                            maskChar, column
                                    )),
                                    functions.expr(String.format(
                                            "right(%s, 4)",
                                            column
                                    ))
                            )
                    )
            );

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to mask credit cards: " + e.getMessage(), e);
        }
    }
}
