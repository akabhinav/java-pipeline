package com.enterprise.pipeline.examples.custom;

import com.enterprise.pipeline.api.Transformation;
import com.enterprise.pipeline.api.TransformationContext;
import com.enterprise.pipeline.api.TransformationException;
import com.enterprise.pipeline.api.ValidationException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Custom transformation example: Mask sensitive data fields.
 *
 * This demonstrates how to create a custom transformation for your specific needs.
 *
 * Config:
 * - columns: List of column names to mask
 * - maskChar: Character to use for masking (default: '*')
 * - visibleChars: Number of characters to leave visible at the end (default: 4)
 *
 * Example:
 * {
 *   "type": "maskSensitiveData",
 *   "config": {
 *     "columns": ["credit_card", "ssn"],
 *     "maskChar": "*",
 *     "visibleChars": 4
 *   }
 * }
 *
 * Result: "1234567890123456" becomes "************3456"
 *
 * @author Enterprise Data Pipeline Team
 */
public class MaskSensitiveDataTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(MaskSensitiveDataTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "maskSensitiveData";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("columns")) {
            throw new ValidationException("'columns' is required");
        }
        if (!(config.get("columns") instanceof List)) {
            throw new ValidationException("'columns' must be a list");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            List<String> columns = (List<String>) config.get("columns");
            String maskChar = config.containsKey("maskChar")
                    ? (String) config.get("maskChar")
                    : "*";
            int visibleChars = config.containsKey("visibleChars")
                    ? ((Number) config.get("visibleChars")).intValue()
                    : 4;

            logger.info("Masking columns: {} (showing last {} chars)", columns, visibleChars);

            Dataset<Row> result = input;

            // Apply masking to each column
            for (String column : columns) {
                // Create masking expression
                // CONCAT(REPEAT('*', LENGTH(col) - visibleChars), RIGHT(col, visibleChars))
                result = result.withColumn(column,
                        functions.when(
                                functions.col(column).isNull(),
                                functions.lit(null)
                        ).otherwise(
                                functions.concat(
                                        functions.expr(String.format(
                                                "repeat('%s', length(%s) - %d)",
                                                maskChar, column, visibleChars
                                        )),
                                        functions.expr(String.format(
                                                "right(%s, %d)",
                                                column, visibleChars
                                        ))
                                )
                        )
                );
            }

            return result;

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to mask sensitive data: " + e.getMessage(), e);
        }
    }
}
